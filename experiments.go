package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// A/B TESTING FRAMEWORK
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY: You can't improve what you can't measure. Every weight in the scoring
// formula (social=0.25, freshness=0.20, etc.) is a guess until you A/B test it.
// TikTok runs hundreds of simultaneous experiments. We need the infrastructure
// now so we can tune as users grow.
//
// HOW IT WORKS:
// 1. Define an experiment with variants (each variant has different weights/configs)
// 2. Users are deterministically assigned to a variant via hash(userId + experimentId) % numVariants
// 3. Every feed request logs which experiment variant was active
// 4. Aggregate metrics per variant to compare performance
//
// The assignment is deterministic — same user always gets same variant for same
// experiment. No cookies or session storage needed.

// Experiment defines a running A/B test.
type Experiment struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Variants    []ExperimentVariant `json:"variants"`
	StartedAt   time.Time           `json:"startedAt"`
	EndedAt     *time.Time          `json:"endedAt,omitempty"`
	Active      bool                `json:"active"`
}

// ExperimentVariant is one arm of the experiment.
type ExperimentVariant struct {
	ID      string                 `json:"id"`      // e.g. "control", "variant_a"
	Name    string                 `json:"name"`     // Human-readable
	Weight  int                    `json:"weight"`   // Traffic allocation (e.g. 50 = 50%)
	Config  map[string]float64     `json:"config"`   // Override scoring weights
}

// ExperimentAssignment is which variant a user is in.
type ExperimentAssignment struct {
	ExperimentID string `json:"experimentId"`
	VariantID    string `json:"variantId"`
	UserID       string `json:"userId"`
}

// defaultExperiments seeds the experiments TABLE on first boot (and
// serves as the in-memory snapshot until the first DB load). The live
// set is DB-backed: edit rows (or POST /api/v1/admin/experiments) and
// every replica picks the change up within experimentsRefreshEvery —
// no redeploy, and setting active=false is the kill switch the old
// hard-coded slice never had.
var defaultExperiments = []Experiment{
	{
		ID:          "scoring_weights_v1",
		Name:        "Social vs Freshness Weight",
		Description: "Test if increasing social weight improves session duration",
		Active:      true,
		StartedAt:   time.Now(),
		Variants: []ExperimentVariant{
			{
				ID: "control", Name: "Current weights", Weight: 50,
				Config: map[string]float64{
					"wSocial": 0.25, "wFreshness": 0.20, "wEnergyFit": 0.20,
					"wRelevance": 0.15, "wQuality": 0.10, "wNovelty": 0.10,
				},
			},
			{
				ID: "variant_a", Name: "Higher social", Weight: 50,
				Config: map[string]float64{
					"wSocial": 0.30, "wFreshness": 0.18, "wEnergyFit": 0.18,
					"wRelevance": 0.14, "wQuality": 0.10, "wNovelty": 0.10,
				},
			},
		},
	},
}

// experimentsStore holds the current []Experiment snapshot. atomic.Value
// because every feed request reads it while the refresher goroutine
// replaces it — a plain global slice would be a data race the moment
// the refresher shipped.
var experimentsStore atomic.Value

func init() { experimentsStore.Store(defaultExperiments) }

// getActiveExperiments returns the current experiment snapshot. Callers
// must treat it as read-only.
func getActiveExperiments() []Experiment {
	v, _ := experimentsStore.Load().([]Experiment)
	return v
}

const experimentsRefreshEvery = 60 * time.Second

// loadExperimentsFromDB replaces the in-memory snapshot from the
// experiments table. On any error the previous snapshot stays — an
// unreachable DB must never turn experiments off mid-flight (variant
// reassignment would contaminate every running measurement).
func loadExperimentsFromDB() {
	if db == nil {
		return
	}
	rows, err := db.Query(`SELECT id, name, description, variants, active, started_at, ended_at FROM experiments`)
	if err != nil {
		log.Printf("experiments: load failed (keeping previous snapshot): %v", err)
		return
	}
	defer rows.Close()
	out := make([]Experiment, 0, 4)
	for rows.Next() {
		var e Experiment
		var variantsRaw []byte
		if err := rows.Scan(&e.ID, &e.Name, &e.Description, &variantsRaw, &e.Active, &e.StartedAt, &e.EndedAt); err != nil {
			continue
		}
		if err := json.Unmarshal(variantsRaw, &e.Variants); err != nil {
			log.Printf("experiments: bad variants JSON for %s — skipping row: %v", e.ID, err)
			continue
		}
		out = append(out, e)
	}
	if len(out) > 0 {
		experimentsStore.Store(out)
	}
}

// seedExperimentsIfEmpty writes defaultExperiments into the table on a
// fresh database so the historical hard-coded experiment continues
// uninterrupted (same ID → same deterministic assignments).
func seedExperimentsIfEmpty() {
	if db == nil {
		return
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM experiments`).Scan(&n); err != nil || n > 0 {
		return
	}
	for _, e := range defaultExperiments {
		variants, err := json.Marshal(e.Variants)
		if err != nil {
			continue
		}
		_, _ = db.Exec(`
			INSERT INTO experiments (id, name, description, variants, active, started_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (id) DO NOTHING`,
			e.ID, e.Name, e.Description, variants, e.Active)
	}
}

// startExperimentRefresher seeds the table if needed, loads once, then
// keeps the snapshot fresh. Called from main() after InitDatabase.
func startExperimentRefresher() {
	seedExperimentsIfEmpty()
	loadExperimentsFromDB()
	go func() {
		t := time.NewTicker(experimentsRefreshEvery)
		defer t.Stop()
		for range t.C {
			loadExperimentsFromDB()
		}
	}()
}

// AdminUpsertExperimentHandler creates or updates one experiment.
// POST /api/v1/admin/experiments with an Experiment JSON body. The
// change is applied to the DB and the local snapshot immediately;
// other replicas converge within experimentsRefreshEvery. Kill switch:
// POST the same experiment with "active": false.
func AdminUpsertExperimentHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	var e Experiment
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil || e.ID == "" {
		http.Error(w, "invalid experiment JSON (id required)", http.StatusBadRequest)
		return
	}
	// Weights must cover the whole population: assignVariant buckets
	// 0-99 against the cumulative weights, so a sum under 100 silently
	// dumps the remainder into the first variant (fallback), skewing
	// the measurement.
	sum := 0
	for _, v := range e.Variants {
		if v.ID == "" {
			http.Error(w, "every variant needs an id", http.StatusBadRequest)
			return
		}
		sum += v.Weight
	}
	if e.Active && sum != 100 {
		http.Error(w, "variant weights must sum to 100 for an active experiment", http.StatusBadRequest)
		return
	}
	variants, err := json.Marshal(e.Variants)
	if err != nil {
		http.Error(w, "variants not serializable", http.StatusBadRequest)
		return
	}
	_, err = db.Exec(`
		INSERT INTO experiments (id, name, description, variants, active, started_at, ended_at)
		VALUES ($1, $2, $3, $4, $5, NOW(), $6)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			variants = EXCLUDED.variants,
			active = EXCLUDED.active,
			ended_at = EXCLUDED.ended_at`,
		e.ID, e.Name, e.Description, variants, e.Active, e.EndedAt)
	if err != nil {
		log.Printf("experiment upsert failed for %s: %v", e.ID, err)
		http.Error(w, "db upsert failed", http.StatusInternalServerError)
		return
	}
	loadExperimentsFromDB()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "id": e.ID})
}

// assignVariant deterministically assigns a user to an experiment variant.
// Uses SHA-256 hash so assignment is uniform and stable.
func assignVariant(userID, experimentID string) string {
	for _, exp := range getActiveExperiments() {
		if exp.ID != experimentID || !exp.Active {
			continue
		}

		// Deterministic hash: same user always gets same variant
		h := sha256.Sum256([]byte(userID + ":" + experimentID))
		bucket := int(binary.BigEndian.Uint32(h[:4])) % 100

		cumulative := 0
		for _, v := range exp.Variants {
			cumulative += v.Weight
			if bucket < cumulative {
				return v.ID
			}
		}
		// Fallback to first variant
		if len(exp.Variants) > 0 {
			return exp.Variants[0].ID
		}
	}
	return "control"
}

// getExperimentConfig returns the scoring weight overrides for a user.
// If no experiment is active, returns nil (use defaults).
func getExperimentConfig(userID string) map[string]float64 {
	for _, exp := range getActiveExperiments() {
		if !exp.Active {
			continue
		}
		variantID := assignVariant(userID, exp.ID)
		for _, v := range exp.Variants {
			if v.ID == variantID {
				return v.Config
			}
		}
	}
	return nil
}

// logExperimentExposure records that a user saw content under a specific variant.
// This is the measurement side — we aggregate these later to compare variants.
//
// One row per (user, experiment, session) — an exposure is a fact about
// the session, not about each feed page in it. The old unconditional
// INSERT grew the table linearly with feed traffic (dozens of identical
// rows per scroll session) while adding nothing statistically, since the
// results query already groups per session. Backed by
// uniq_experiment_exposures_session (see runMigrations).
func logExperimentExposure(userID, experimentID, variantID, sessionID string) {
	_, err := db.Exec(`
		INSERT INTO experiment_exposures (user_id, experiment_id, variant_id, session_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, experiment_id, session_id) DO NOTHING`,
		userID, experimentID, variantID, sessionID)
	if err != nil {
		log.Printf("Failed to log experiment exposure: %v", err)
	}
}

// ExperimentResultsHandler returns aggregate metrics per variant.
// GET /api/v1/experiments/results?experimentId=X
func ExperimentResultsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	experimentID := r.URL.Query().Get("experimentId")
	if experimentID == "" {
		// Return all active experiments
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"experiments": getActiveExperiments(),
		})
		return
	}

	// Aggregate metrics per variant
	type VariantMetrics struct {
		VariantID        string  `json:"variantId"`
		UniqueUsers      int     `json:"uniqueUsers"`
		TotalSessions    int     `json:"totalSessions"`
		AvgSessionItems  float64 `json:"avgSessionItems"`
		AvgCompletionRate float64 `json:"avgCompletionRate"`
		AvgSkipRate      float64 `json:"avgSkipRate"`
		AvgLikesPerSession float64 `json:"avgLikesPerSession"`
		AvgSharesPerSession float64 `json:"avgSharesPerSession"`
	}

	rows, err := db.Query(`
		SELECT ee.variant_id,
			COUNT(DISTINCT ee.user_id) as unique_users,
			COUNT(DISTINCT ee.session_id) as total_sessions,
			COALESCE(AVG(sess.items), 0) as avg_items,
			COALESCE(AVG(sess.completion), 0) as avg_completion,
			COALESCE(AVG(sess.skip_rate), 0) as avg_skip_rate,
			COALESCE(AVG(sess.likes), 0) as avg_likes,
			COALESCE(AVG(sess.shares), 0) as avg_shares
		FROM experiment_exposures ee
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*) as items,
				AVG(fe.completion_rate) FILTER (WHERE fe.event_type = 'view') as completion,
				CAST(COUNT(*) FILTER (WHERE fe.event_type = 'skip') AS FLOAT) /
					NULLIF(COUNT(*), 0) as skip_rate,
				COUNT(*) FILTER (WHERE fe.event_type = 'like') as likes,
				COUNT(*) FILTER (WHERE fe.event_type = 'share') as shares
			FROM feed_events fe
			WHERE fe.user_id = ee.user_id AND fe.session_id = ee.session_id
		) sess ON true
		WHERE ee.experiment_id = $1
		GROUP BY ee.variant_id
		ORDER BY ee.variant_id`, experimentID)

	if err != nil {
		http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []VariantMetrics
	for rows.Next() {
		var m VariantMetrics
		rows.Scan(&m.VariantID, &m.UniqueUsers, &m.TotalSessions,
			&m.AvgSessionItems, &m.AvgCompletionRate, &m.AvgSkipRate,
			&m.AvgLikesPerSession, &m.AvgSharesPerSession)
		results = append(results, m)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"experimentId": experimentID,
		"variants":     results,
	})
}

// ExperimentsListHandler returns all experiments.
// GET /api/v1/experiments
func ExperimentsListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"experiments": getActiveExperiments(),
	})
}
