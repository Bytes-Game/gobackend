package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
)

// ════════════════════════════════════════════════════════════════════════════════
// ADMIN DASHBOARDS — funnels & errors
// ════════════════════════════════════════════════════════════════════════════════
//
// Simple internal endpoints backed by the existing feed_events table. No
// warehouse needed; Postgres handles the aggregation under the current scale.
//
// Endpoints:
//   GET /api/v1/admin/funnels    → JSON: upload funnels per uploadType (7d window)
//   GET /api/v1/admin/errors     → JSON: error counts by surface + errorType (7d)
//   GET /admin                   → inline HTML dashboard that fetches above
//
// Auth: intentionally NOT implemented here — wire auth middleware via whatever
// pattern the rest of the admin surface uses (e.g. /admin/reseed). Left as a
// TODO to avoid inventing an auth story.

// AdminFunnelsHandler returns per-uploadType funnel counts for the last 7 days.
// Response shape:
//
//	{
//	  "challenge": {
//	    "start": 123, "step_events": 456, "abandon": 40, "complete": 70,
//	    "completionRate": 0.569, "abandonByStep": {"video": 10, "details": 22, ...}
//	  },
//	  "post": {...}, "response": {...}
//	}
func AdminFunnelsHandler(w http.ResponseWriter, r *http.Request) {
	types := []string{"challenge", "post", "response"}
	result := make(map[string]map[string]interface{}, len(types))

	for _, t := range types {
		var start, step, abandon, complete int
		row := db.QueryRow(`
			SELECT
			  COUNT(*) FILTER (WHERE event_type='upload_start'    AND metadata->>'uploadType'=$1) AS start,
			  COUNT(*) FILTER (WHERE event_type='upload_step'     AND metadata->>'uploadType'=$1) AS step,
			  COUNT(*) FILTER (WHERE event_type='upload_abandon'  AND metadata->>'uploadType'=$1) AS abandon,
			  COUNT(*) FILTER (WHERE event_type='upload_complete' AND metadata->>'uploadType'=$1) AS complete
			FROM feed_events
			WHERE created_at > NOW() - INTERVAL '7 days'
		`, t)
		if err := row.Scan(&start, &step, &abandon, &complete); err != nil && err != sql.ErrNoRows {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		completionRate := 0.0
		if start > 0 {
			completionRate = float64(complete) / float64(start)
		}

		byStep := make(map[string]int)
		if rows, err := db.Query(`
			SELECT COALESCE(metadata->>'atStep',''), COUNT(*)
			FROM feed_events
			WHERE event_type = 'upload_abandon'
			  AND metadata->>'uploadType' = $1
			  AND created_at > NOW() - INTERVAL '7 days'
			GROUP BY metadata->>'atStep'
		`, t); err == nil {
			for rows.Next() {
				var s string
				var n int
				if rows.Scan(&s, &n) == nil && s != "" {
					byStep[s] = n
				}
			}
			rows.Close()
		}

		result[t] = map[string]interface{}{
			"start":          start,
			"step_events":    step,
			"abandon":        abandon,
			"complete":       complete,
			"completionRate": completionRate,
			"abandonByStep":  byStep,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// AdminErrorsHandler returns error counts grouped by (surface, errorType) for
// the last 7 days — the top-100 loudest buckets.
func AdminErrorsHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT
		  COALESCE(metadata->>'surface','unknown')   AS surface,
		  COALESCE(metadata->>'errorType','unknown') AS error_type,
		  COUNT(*)                                   AS c
		FROM feed_events
		WHERE event_type = 'error'
		  AND created_at > NOW() - INTERVAL '7 days'
		GROUP BY surface, error_type
		ORDER BY c DESC
		LIMIT 100
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type rec struct {
		Surface   string `json:"surface"`
		ErrorType string `json:"errorType"`
		Count     int    `json:"count"`
	}
	out := []rec{}
	for rows.Next() {
		var r rec
		if rows.Scan(&r.Surface, &r.ErrorType, &r.Count) == nil {
			out = append(out, r)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// AdminHealthHandler returns the last analytics batch run: when it started,
// how long it took, which sub-jobs ran, how many users each covered, any errors.
// Use this to spot silent nightly failures.
func AdminHealthHandler(w http.ResponseWriter, r *http.Request) {
	h := SnapshotAnalyticsHealth()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h)
}

// AdminGoldenHourHandler exposes the analytics-computed golden notification
// hour for a given user. Useful when debugging "why isn't this user getting
// pushes at the expected time" — you can confirm we have a signal and how
// confident it is before looking at the delivery pipeline.
//
// Response: {"hour": 19, "confidence": 0.78} or {"hour": -1, "confidence": 0}
// if no data has been computed yet (new user / analytics hasn't run).
func AdminGoldenHourHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	if userID == "" {
		http.Error(w, "userId required", http.StatusBadRequest)
		return
	}
	hour, conf := GetGoldenHour(userID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"hour":       hour,
		"confidence": conf,
	})
}

// AdminOnlineUsersHandler lists usernames currently holding an open websocket.
// Useful for on-call verification that push delivery is reaching active
// sessions and for manual sampling during incident triage.
//
// Response: {"count": 12, "usernames": ["alice","bob",...]}
func AdminOnlineUsersHandler(w http.ResponseWriter, r *http.Request) {
	names := GetOnlineUsernames()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"count":     len(names),
		"usernames": names,
	})
}

// AdminDashboardHandler serves an inline HTML page that fetches the JSON
// endpoints and renders them as tables. Kept inline so deployment stays a
// single binary with no separate static assets to serve.
func AdminDashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, adminDashboardHTML)
}

const adminDashboardHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>devf admin</title>
<style>
body{font-family:system-ui,sans-serif;background:#0b0b0e;color:#eee;margin:0;padding:24px;max-width:1100px;margin:0 auto}
h1{font-size:20px;letter-spacing:.5px;color:#8ab4ff}
h2{margin-top:32px;font-size:15px;color:#ffb}
table{border-collapse:collapse;width:100%;background:#15161b;margin-top:8px;font-size:13px}
th,td{border:1px solid #2a2b33;padding:6px 10px;text-align:left;vertical-align:top}
th{background:#1b1c23;color:#aab}
tr:nth-child(even) td{background:#101116}
.tiny{color:#7a7c85;font-size:11px;margin-top:4px}
code{background:#222;padding:1px 5px;border-radius:3px;color:#ffb}
.pct{font-weight:600}
.pct.low{color:#ff8a8a}
.pct.mid{color:#ffd27a}
.pct.hi{color:#8af0a8}
</style></head><body>
<h1>devf — funnels & errors</h1>
<div class="tiny">Windows: last 7 days. Pulled live from feed_events. Reload to refresh.</div>

<h2>Nightly analytics job</h2>
<div id="health">loading…</div>

<h2>Upload funnels</h2>
<div id="funnels">loading…</div>

<h2>Errors by surface</h2>
<div id="errors">loading…</div>

<script>
function rateClass(r){if(r<0.3)return 'low';if(r<0.6)return 'mid';return 'hi'}
function fmtAgo(iso){
  if(!iso||iso.startsWith('0001-01-01'))return 'never';
  const t=new Date(iso), d=Date.now()-t.getTime();
  if(d<60000)return Math.floor(d/1000)+'s ago';
  if(d<3600000)return Math.floor(d/60000)+'m ago';
  return Math.floor(d/3600000)+'h ago';
}
async function render(){
  try{
    const h = await fetch('/api/v1/admin/health').then(r=>r.json());
    const stale = h.startedAt && (Date.now()-new Date(h.startedAt).getTime() > 30*3600*1000);
    let head = '<div class="tiny">run #'+(h.runCount||0)+' · started '+fmtAgo(h.startedAt)+' · took '+(h.duration||'—');
    if(stale) head += ' · <span class="pct low">STALE (>30h)</span>';
    head += '</div>';
    let hh = head + '<table><tr><th>job</th><th>users</th><th>duration</th><th>error</th></tr>';
    for(const name of ['tie_strength','social_drive','creator_affinity','page_dwell']){
      const r = (h.results||{})[name] || {};
      const errCell = r.err ? '<span class="pct low">'+r.err+'</span>' : 'ok';
      hh += '<tr><td><code>'+name+'</code></td><td>'+(r.users||0)+'</td><td>'+(r.duration||'—')+'</td><td>'+errCell+'</td></tr>';
    }
    hh += '</table>';
    document.getElementById('health').innerHTML = hh;
  }catch(e){document.getElementById('health').textContent='error: '+e}

  try{
    const f = await fetch('/api/v1/admin/funnels').then(r=>r.json());
    let html = '<table><tr><th>type</th><th>start</th><th>complete</th><th>abandon</th><th>rate</th><th>abandon by step</th></tr>';
    for(const k of Object.keys(f)){
      const b = f[k];
      const byStep = Object.entries(b.abandonByStep||{}).map(([s,n])=>s+': '+n).join(', ') || '—';
      const pct = (b.completionRate*100).toFixed(1)+'%';
      const cls = rateClass(b.completionRate);
      html += '<tr><td><code>'+k+'</code></td><td>'+b.start+'</td><td>'+b.complete+'</td><td>'+b.abandon+'</td><td class="pct '+cls+'">'+pct+'</td><td>'+byStep+'</td></tr>';
    }
    html += '</table>';
    document.getElementById('funnels').innerHTML = html;
  }catch(e){document.getElementById('funnels').textContent='error: '+e}

  try{
    const e = await fetch('/api/v1/admin/errors').then(r=>r.json());
    let eh = '<table><tr><th>surface</th><th>errorType</th><th>count</th></tr>';
    for(const r of (e||[])) eh += '<tr><td>'+r.surface+'</td><td>'+r.errorType+'</td><td>'+r.count+'</td></tr>';
    if(!e||!e.length) eh += '<tr><td colspan="3" style="color:#7a7c85">no errors in window</td></tr>';
    eh += '</table>';
    document.getElementById('errors').innerHTML = eh;
  }catch(e){document.getElementById('errors').textContent='error: '+e}
}
render();
</script>
</body></html>`
