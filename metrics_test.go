package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Metrics live in package-level globals — register once across the test run.
var metricsOnce sync.Once

func ensureMetricsRegistered(t *testing.T) {
	t.Helper()
	metricsOnce.Do(func() {
		registerMetrics()
	})
}

func TestMetrics_EndpointReturnsPrometheusFormat(t *testing.T) {
	resetRedis(t)
	ensureMetricsRegistered(t)

	// Counters only render in /metrics output after at least one observation.
	// Emit one of each so the scrape surface is complete.
	MarkBlocked("u_probe", "c_probe")
	loadBandit("u_probe").updateArm("u_probe", "s", 1.0)
	metricLTRFlushes.WithLabelValues("ok").Inc()
	metricAnalyticsJob.WithLabelValues("probe", "ok").Inc()
	metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/feed/smart?userId=x", nil))

	ts := httptest.NewServer(MetricsHandler())
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/metrics returned %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	wantFamilies := []string{
		"devf_http_requests_total",
		"devf_bandit_writes_total",
		"devf_ltr_flushes_total",
		"devf_signal_capture_total",
		"devf_analytics_job_total",
		"devf_feed_latency_seconds",
	}
	for _, w := range wantFamilies {
		if !strings.Contains(s, w) {
			t.Errorf("/metrics missing family %q", w)
		}
	}
}

func TestMetrics_SignalCaptureIncrementsCounter(t *testing.T) {
	resetRedis(t)
	ensureMetricsRegistered(t)
	c := metricSignalCapture.WithLabelValues("block")
	before := testutil.ToFloat64(c)
	MarkBlocked("u1", "c1")
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("MarkBlocked should +1 block counter; before=%v after=%v", before, after)
	}
}

func TestMetrics_UnfollowIncrementsCounter(t *testing.T) {
	resetRedis(t)
	ensureMetricsRegistered(t)
	c := metricSignalCapture.WithLabelValues("unfollow")
	before := testutil.ToFloat64(c)
	MarkUnfollowed("u1", "c1")
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("MarkUnfollowed should +1 unfollow counter; before=%v after=%v", before, after)
	}
}

func TestMetrics_SearchIncrementsCounter(t *testing.T) {
	resetRedis(t)
	ensureMetricsRegistered(t)
	c := metricSignalCapture.WithLabelValues("search")
	before := testutil.ToFloat64(c)
	RecordSearchQuery("u1", "hello world")
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("RecordSearchQuery should +1 search counter; before=%v after=%v", before, after)
	}
}

func TestMetrics_BanditWriteIncrementsCounter(t *testing.T) {
	resetRedis(t)
	ensureMetricsRegistered(t)
	c := metricBanditWrites.WithLabelValues("ok")
	before := testutil.ToFloat64(c)
	b := loadBandit("uM1")
	b.updateArm("uM1", "strategy_z", 1.0)
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("bandit write should +1 ok counter; before=%v after=%v", before, after)
	}
}

func TestMetrics_LTRFlushIncrementsCounter(t *testing.T) {
	resetRedis(t)
	resetLTR()
	ensureMetricsRegistered(t)
	// Train the Engaged cohort with 5 obs so flush has something dirty.
	bd := map[string]float64{"quality": 0.5}
	for i := 0; i < 5; i++ {
		ltrObserve(CohortEngaged, bd, 1.0)
	}
	c := metricLTRFlushes.WithLabelValues("ok")
	before := testutil.ToFloat64(c)
	ltrFlush()
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("ltrFlush should +1 ok counter; before=%v after=%v", before, after)
	}
	// Updates gauge should reflect the 5 observations.
	g := metricLTRUpdates.WithLabelValues(string(CohortEngaged))
	if testutil.ToFloat64(g) < 5 {
		t.Errorf("LTR updates gauge should be >=5, got %v", testutil.ToFloat64(g))
	}
}

func TestMetrics_MiddlewareRecordsRequest(t *testing.T) {
	ensureMetricsRegistered(t)
	handler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	c := metricHTTPRequests.WithLabelValues("/health", "GET", "2xx")
	before := testutil.ToFloat64(c)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("middleware should record /health request; before=%v after=%v", before, after)
	}
}

func TestMetrics_MiddlewareRecordsFeedLatency(t *testing.T) {
	ensureMetricsRegistered(t)
	handler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("{}"))
	}))
	// The feed-requests counter increments per feed endpoint hit.
	c := metricFeedRequests.WithLabelValues("/api/v1/feed/smart", "2xx")
	before := testutil.ToFloat64(c)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/feed/smart?userId=x", nil))
	after := testutil.ToFloat64(c)
	if after-before < 0.999 {
		t.Errorf("feed endpoint hit should bump feed_requests; before=%v after=%v", before, after)
	}
}

func TestMetrics_StatusClass(t *testing.T) {
	cases := map[int]string{200: "2xx", 201: "2xx", 302: "3xx", 404: "4xx", 401: "4xx", 500: "5xx", 503: "5xx"}
	for code, want := range cases {
		if got := statusClass(code); got != want {
			t.Errorf("statusClass(%d) = %q, want %q", code, got, want)
		}
	}
}

func TestMetrics_PathCollapseForIDs(t *testing.T) {
	cases := map[string]string{
		"/api/v1/users/alice":         "/api/v1/users/:username",
		"/api/v1/posts/u_12345":       "/api/v1/posts/:userId",
		"/api/v1/challenges/ch_9001":  "/api/v1/challenges/:id",
		"/api/v1/chat/messages/u1/u2": "/api/v1/chat/messages/:userId",
		"/ws/alice":                   "/ws/:username",
		"/api/v1/feed/smart":          "/api/v1/feed/smart",
		"/metrics":                    "/metrics",
		"/something/unknown":          "other",
	}
	for in, want := range cases {
		if got := normalizeMetricPath(in); got != want {
			t.Errorf("normalizeMetricPath(%q) = %q, want %q", in, got, want)
		}
	}
}
