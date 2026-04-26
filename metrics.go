package main

import (
	"bufio"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ════════════════════════════════════════════════════════════════════════════════
// PROMETHEUS METRICS
// ════════════════════════════════════════════════════════════════════════════════
//
// Exposed at /metrics — scrape at 15s intervals from Prometheus. The metrics
// cover the four places we most care about observing in production:
//
//   1) Feed requests — latency histogram + count by status.
//   2) Bandit writes — pass/fail counters so a silent Redis blip is visible.
//   3) LTR flush — success/fail counters + observed-updates gauge per cohort.
//   4) Negative-signal captures — counters per signal type.
//
// Everything here is non-blocking and never returns errors to the caller. If
// Prometheus isn't scraping yet, metrics just accumulate in memory.

var (
	metricFeedRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_feed_requests_total",
			Help: "Count of feed requests by endpoint and status.",
		},
		[]string{"endpoint", "status"},
	)

	metricFeedLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "devf_feed_latency_seconds",
			Help:    "Latency of feed-serving endpoints.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"endpoint"},
	)

	metricBanditWrites = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_bandit_writes_total",
			Help: "Thompson-sampling bandit writes by outcome.",
		},
		[]string{"outcome"}, // "ok" | "error"
	)

	metricLTRFlushes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_ltr_flushes_total",
			Help: "LTR weight flushes to Redis by outcome.",
		},
		[]string{"outcome"},
	)

	metricLTRUpdates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "devf_ltr_updates",
			Help: "Total observed training examples per cohort (gauge).",
		},
		[]string{"cohort"},
	)

	metricSignalCapture = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_signal_capture_total",
			Help: "Negative / search signals captured by type.",
		},
		[]string{"kind"}, // "block" | "unfollow" | "bounce" | "search" | "session_end"
	)

	metricAnalyticsJob = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_analytics_job_total",
			Help: "Analytics sub-job runs by name and outcome.",
		},
		[]string{"job", "outcome"},
	)

	metricHTTPRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_http_requests_total",
			Help: "All HTTP requests by path and status.",
		},
		[]string{"path", "method", "status"},
	)

	metricHTTPLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "devf_http_latency_seconds",
			Help:    "Latency of HTTP handlers.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"path", "method"},
	)

	// ── Embeddings / two-tower ───────────────────────────────────────────────
	metricEmbedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_embed_updates_total",
			Help: "User-embedding EMA updates by outcome.",
		},
		[]string{"outcome"}, // "ok" | "error" | "skip_cold"
	)
	metricEmbedCacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_embed_cache_total",
			Help: "Content-embedding cache hits vs misses.",
		},
		[]string{"outcome"}, // "hit" | "miss"
	)

	// ── Seen-content filter ──────────────────────────────────────────────────
	metricSeenMarks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_seen_marks_total",
			Help: "Impressions recorded into the per-user seen set.",
		},
		[]string{"outcome"}, // "ok" | "error"
	)
	metricSeenFiltered = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "devf_seen_filtered_total",
			Help: "Candidates dropped because the user had seen them recently.",
		},
	)

	// ── MMR diversity re-rank ────────────────────────────────────────────────
	metricMMRReranks = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "devf_mmr_reranks_total",
			Help: "Number of MMR re-rank passes performed.",
		},
	)

	// ── Platt calibration fits ───────────────────────────────────────────────
	metricPlattFits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "devf_platt_fits_total",
			Help: "Platt-calibrator refits executed.",
		},
	)

	// ── Multi-source candidate generation ────────────────────────────────────
	metricCandidateSource = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_candidate_source_total",
			Help: "Candidates produced by each retrieval source.",
		},
		[]string{"source", "outcome"}, // source: recency|trending|trendingRealtime|follow|collab|embed ; outcome: ok|error|empty|panic
	)

	// ── Cold-start bootstrap pool ────────────────────────────────────────────
	metricBootstrapPool = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_bootstrap_pool_total",
			Help: "Bootstrap-pool worker activity: compute=refresh runs, inject=items mixed into cold-user feeds.",
		},
		[]string{"action"}, // "compute" | "inject"
	)

	// ── Watch-ratio prediction head ──────────────────────────────────────────
	metricWatchRatioObserve = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_watch_ratio_observe_total",
			Help: "Watch-ratio training samples by cohort.",
		},
		[]string{"cohort"},
	)
	metricWatchRatioFlush = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_watch_ratio_flush_total",
			Help: "Watch-ratio per-cohort weight flushes by outcome.",
		},
		[]string{"outcome"},
	)

	// ── Surprise injection (filter-bubble defense) ───────────────────────────
	metricSurpriseInject = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_surprise_inject_total",
			Help: "Surprise wildcard injections by outcome.",
		},
		[]string{"outcome"}, // "ok" | "skipped_atrisk" | "no_pool" | "no_unfamiliar"
	)

	// ── Push notifications: enqueue + dispatch outcomes ──────────────────────
	metricNotifEnqueue = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_notif_enqueue_total",
			Help: "Notification enqueue outcomes by trigger and decision.",
		},
		[]string{"trigger", "outcome"}, // outcome: queued|opted_out|rate_limited|deduped
	)
	metricNotifDispatch = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_notif_dispatch_total",
			Help: "Notification dispatch outcomes by trigger and result.",
		},
		[]string{"trigger", "result"}, // result: sent|failed|no_tokens
	)

	// ── Creator insights API hits ────────────────────────────────────────────
	metricCreatorInsights = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_creator_insights_total",
			Help: "Creator-insights endpoint hits by scope.",
		},
		[]string{"scope"}, // overview | per_content
	)

	// ── Peak upgrades ────────────────────────────────────────────────────────
	metricTwoTowerUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_two_tower_updates_total",
			Help: "Trained two-tower content-vector SGD updates by outcome.",
		},
		[]string{"outcome"}, // pos | neg
	)
	metricNegProfileMine = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_neg_profile_mine_total",
			Help: "Negative-feedback profile-mining nudges by event type.",
		},
		[]string{"event"}, // block | unfollow | not_interested | skip | bounce
	)
	metricCreatorResidualUpdate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "devf_creator_residual_updates_total",
			Help: "Per-creator reach-residual EMA updates.",
		},
	)
	metricCohortBlendObserve = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devf_cohort_blend_observe_total",
			Help: "Cohort×source blending reward observations.",
		},
		[]string{"cohort", "source"},
	)
)

// registerMetrics is called from main() — safe to call multiple times, each
// Register is idempotent on the custom registry we use.
func registerMetrics() {
	prometheus.MustRegister(
		metricFeedRequests,
		metricFeedLatency,
		metricBanditWrites,
		metricLTRFlushes,
		metricLTRUpdates,
		metricSignalCapture,
		metricAnalyticsJob,
		metricHTTPRequests,
		metricHTTPLatency,
		metricEmbedUpdates,
		metricEmbedCacheHits,
		metricSeenMarks,
		metricSeenFiltered,
		metricMMRReranks,
		metricPlattFits,
		metricCandidateSource,
		metricBootstrapPool,
		metricWatchRatioObserve,
		metricWatchRatioFlush,
		metricSurpriseInject,
		metricNotifEnqueue,
		metricNotifDispatch,
		metricCreatorInsights,
		metricTwoTowerUpdates,
		metricNegProfileMine,
		metricCreatorResidualUpdate,
		metricCohortBlendObserve,
	)
}

// metricsMiddleware wraps every HTTP handler so every request contributes to
// the global counters. Uses a small statusRecorder to capture the response code.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(sr, r)
		dur := time.Since(start).Seconds()

		// Strip dynamic path segments to keep label cardinality bounded.
		path := normalizeMetricPath(r.URL.Path)
		status := statusClass(sr.status)
		metricHTTPRequests.WithLabelValues(path, r.Method, status).Inc()
		metricHTTPLatency.WithLabelValues(path, r.Method).Observe(dur)

		// Feed endpoints get their own detailed histogram too.
		switch path {
		case "/api/v1/feed/smart", "/api/v1/feed/following", "/api/v1/feed/recommended", "/api/v1/feed/following/v2":
			metricFeedRequests.WithLabelValues(path, status).Inc()
			metricFeedLatency.WithLabelValues(path).Observe(dur)
		}
	})
}

// statusRecorder captures the response status code for metrics. Implements
// http.ResponseWriter by delegating to the embedded writer.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Hijack passes through to the underlying ResponseWriter so WebSocket
// upgrades work through the metrics middleware. Without this, every
// gorilla/websocket Upgrade() call fails with "response writer does not
// implement http.Hijacker" — which surfaces in the Flutter client as
// "Connection ... was not upgraded to websocket". Net = the WS handshake
// never completes and clients reconnect-loop forever.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := s.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

// Flush passes through Flusher for SSE / streaming endpoints (some admin
// JSON streams use this). Same rationale as Hijack — middleware mustn't
// break interface contracts the underlying writer supports.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// statusClass flattens a numeric status to a class (2xx, 4xx, 5xx) so label
// cardinality is bounded.
func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}

// normalizeMetricPath strips dynamic segments (IDs) from HTTP paths so every
// request on /api/v1/users/{username} labels as /api/v1/users/:username.
// This prevents Prometheus cardinality explosion on paths with user-supplied IDs.
func normalizeMetricPath(p string) string {
	// Exact matches first (hot path, no allocation).
	switch p {
	case "/health", "/api/v1/users", "/api/v1/feed", "/api/v1/home",
		"/api/v1/feed/smart", "/api/v1/feed/following", "/api/v1/feed/following/v2",
		"/api/v1/feed/recommended", "/api/v1/follow", "/api/v1/unfollow",
		"/api/v1/like", "/api/v1/comments", "/api/v1/challenges",
		"/api/v1/challenges/arena", "/api/v1/challenges/friends",
		"/api/v1/challenges/accept", "/api/v1/challenges/like",
		"/api/v1/challenges/vote", "/api/v1/challenges/comments",
		"/api/v1/categories", "/api/v1/events", "/api/v1/events/batch",
		"/api/v1/profile", "/api/v1/experiments", "/api/v1/experiments/results",
		"/api/v1/users/similar", "/api/v1/watch", "/api/v1/report",
		"/api/v1/admin/reseed", "/api/v1/admin/funnels", "/api/v1/admin/errors",
		"/api/v1/admin/health", "/api/v1/admin/golden_hour", "/api/v1/admin/diagnostics",
		"/api/v1/chat/send", "/api/v1/chat/read", "/api/v1/chat/edit",
		"/api/v1/chat/delete", "/api/v1/chat/forward", "/api/v1/save",
		"/login", "/search", "/admin", "/metrics":
		return p
	}
	// Dynamic segments (IDs) — collapse to a label-safe form.
	return collapseDynamicPath(p)
}

// collapseDynamicPath walks the path and replaces segments that look like
// IDs with fixed placeholders. This is a best-effort cardinality control.
func collapseDynamicPath(p string) string {
	// Fast pre-check: if there's nothing after /api/v1/, just return as-is.
	if len(p) < 2 {
		return p
	}
	// Common patterns: /api/v1/users/{username}, /api/v1/posts/{userId}, …
	// Rather than full parsing, we bucket a handful of known prefixes.
	prefixes := []struct{ prefix, collapsed string }{
		{"/api/v1/users/", "/api/v1/users/:username"},
		{"/api/v1/posts/", "/api/v1/posts/:userId"},
		{"/api/v1/comments/", "/api/v1/comments/:postId"},
		{"/api/v1/challenges/", "/api/v1/challenges/:id"},
		{"/api/v1/chat/conversations/", "/api/v1/chat/conversations/:userId"},
		{"/api/v1/chat/messages/", "/api/v1/chat/messages/:userId"},
		{"/api/v1/chat/online/", "/api/v1/chat/online/:username"},
		{"/api/v1/saved/", "/api/v1/saved/:userId"},
		{"/ws/", "/ws/:username"},
	}
	for _, p2 := range prefixes {
		if len(p) > len(p2.prefix) && p[:len(p2.prefix)] == p2.prefix {
			return p2.collapsed
		}
	}
	return "other"
}

// MetricsHandler returns the Prometheus /metrics handler.
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
