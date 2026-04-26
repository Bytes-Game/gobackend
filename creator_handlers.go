package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// ─────────────────────────────────────────────────────────────────────────────
// CREATOR INSIGHTS HTTP HANDLERS
//
//   GET /api/v1/creator/insights?creatorId=X&windowDays=30
//        → CreatorOverview JSON
//
//   GET /api/v1/creator/insights/content?creatorId=X&type=challenge&id=Y&windowDays=30
//        → CreatorPerContent JSON (only the content's owner can read this)
// ─────────────────────────────────────────────────────────────────────────────

// HandleCreatorInsightsOverview returns the dashboard summary for one creator.
func HandleCreatorInsightsOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	creatorID := r.URL.Query().Get("creatorId")
	if creatorID == "" {
		http.Error(w, "creatorId required", http.StatusBadRequest)
		return
	}
	windowDays := parseIntDefault(r.URL.Query().Get("windowDays"), 30)

	overview, err := buildCreatorOverview(creatorID, windowDays)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if metricCreatorInsights != nil {
		metricCreatorInsights.WithLabelValues("overview").Inc()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(overview)
}

// HandleCreatorInsightsPerContent returns the deep-dive insights for one
// piece. Caller must own the content (enforced inside buildCreatorPerContent).
func HandleCreatorInsightsPerContent(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	creatorID := r.URL.Query().Get("creatorId")
	contentType := r.URL.Query().Get("type")
	contentID := r.URL.Query().Get("id")
	if creatorID == "" || contentType == "" || contentID == "" {
		http.Error(w, "creatorId, type, id required", http.StatusBadRequest)
		return
	}
	windowDays := parseIntDefault(r.URL.Query().Get("windowDays"), 30)

	insights, err := buildCreatorPerContent(creatorID, contentType, contentID, windowDays)
	if err != nil {
		if err.Error() == "forbidden" {
			http.Error(w, "this content does not belong to you", http.StatusForbidden)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if metricCreatorInsights != nil {
		metricCreatorInsights.WithLabelValues("per_content").Inc()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(insights)
}

// parseIntDefault parses the query value or returns def on any failure.
func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
