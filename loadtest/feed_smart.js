// k6 load test for devb /feed/smart
//
// Install k6: https://k6.io/docs/get-started/installation/
// Run: k6 run -e BASE=https://api.example.com loadtest/feed_smart.js
//
// Mirrors the native Go loadtest but gives you k6's richer output (InfluxDB,
// Grafana, JSON summary) and built-in thresholds that fail the run if SLOs
// are breached.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const feedLatency = new Trend('feed_latency_ms');
const eventLatency = new Trend('event_latency_ms');
const errorRate = new Rate('errors');

export const options = {
  scenarios: {
    // Ramp up and hold — representative of a real traffic spike.
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 200 },   // ramp to 200 VUs
        { duration: '2m',  target: 200 },   // hold
        { duration: '30s', target: 500 },   // spike to 500 VUs
        { duration: '2m',  target: 500 },   // hold peak
        { duration: '30s', target: 0 },     // ramp down
      ],
    },
  },
  thresholds: {
    // Hard SLOs — if violated the k6 run exits non-zero.
    'feed_latency_ms':    ['p(95)<500',  'p(99)<1500'],
    'event_latency_ms':   ['p(95)<100',  'p(99)<500'],
    'errors':             ['rate<0.01'],   // <1% error rate
    'http_req_failed':    ['rate<0.01'],
  },
};

const BASE = __ENV.BASE || 'http://localhost:8081';

export default function () {
  const userId = `k6_user_${__VU}_${__ITER}`;

  // 1. Fetch the smart feed.
  const feedRes = http.get(`${BASE}/api/v1/feed/smart?userId=${userId}`);
  feedLatency.add(feedRes.timings.duration);
  const feedOk = check(feedRes, {
    'feed status 200':   r => r.status === 200,
    'feed body non-empty': r => r.body && r.body.length > 0,
  });
  errorRate.add(!feedOk);

  // 2. Emit a view event — exercises the ingest + LTR stash lookup path.
  const evt = JSON.stringify({
    userId: userId,
    eventType: 'view',
    contentType: 'post',
    contentId: `k6_post_${Math.floor(Math.random() * 1000)}`,
    watchDurationMs: 2000 + Math.floor(Math.random() * 4000),
    completionRate: Math.random(),
  });
  const evtRes = http.post(`${BASE}/api/v1/events`, evt, {
    headers: { 'Content-Type': 'application/json' },
  });
  eventLatency.add(evtRes.timings.duration);
  const evtOk = check(evtRes, {
    'event status 200': r => r.status === 200,
  });
  errorRate.add(!evtOk);

  // Micro-sleep to keep request pattern realistic (users don't hammer).
  sleep(0.5 + Math.random() * 2);
}

export function handleSummary(data) {
  return {
    stdout: textSummary(data),
    'loadtest/summary.json': JSON.stringify(data, null, 2),
  };
}

// Minimal text summary — if you have k6's own summary formatter installed
// import it from `https://jslib.k6.io/k6-summary/0.0.2/index.js` instead.
function textSummary(data) {
  const m = data.metrics;
  let out = '\n── k6 summary ──\n';
  out += `  iterations:         ${m.iterations?.values?.count || 0}\n`;
  out += `  http_req_failed:    ${pct(m.http_req_failed?.values?.rate)}\n`;
  out += `  feed p95 / p99:     ${fix(m.feed_latency_ms?.values?.['p(95)'])} / ${fix(m.feed_latency_ms?.values?.['p(99)'])} ms\n`;
  out += `  event p95 / p99:    ${fix(m.event_latency_ms?.values?.['p(95)'])} / ${fix(m.event_latency_ms?.values?.['p(99)'])} ms\n`;
  return out;
}

function fix(v) { return v === undefined ? '?' : Number(v).toFixed(0); }
function pct(v) { return v === undefined ? '?' : (100 * v).toFixed(2) + '%'; }
