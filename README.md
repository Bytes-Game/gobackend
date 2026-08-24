# gobackend — Battle Arena API

The Go server behind the Battle Arena app. It serves the feed, the challenge
and battle system, chat, search, notifications, and media upload signing.

The Flutter client lives in a separate repo (`Frontend`).

---

## ⚠ READ THIS BEFORE GOING LIVE

**If you are a person or a model picking this repo up, start here.**

Two things are written, tested and switched OFF. Neither is unfinished. Both
are off because switching them on is somebody's decision to make, not the
code's. One of them **must** be dealt with before real users arrive. The other
can wait as long as you like.

| | what | before launch? |
|---|---|---|
| 1 | uploads can wait up to 30 minutes to become playable | **YES — must fix** |
| 2 | the worker does not listen to speech | no, whenever you want |

### 1. Uploads can wait up to 30 minutes ⚠ MUST FIX BEFORE LAUNCH

A video is not watchable until the worker converts it, and the worker runs on
a timer — every 30 minutes, in `.github/workflows/hls-worker.yml`. Post a
video a minute after a run finishes and it is invisible for twenty-nine more.

**This is fine for testing and unacceptable in production.** The person who
just posted does not know what a queue is. They know their video is not there,
and they will not post a second one.

The fix is written and merged and needs one secret to switch on. The backend
asks GitHub to start the worker the moment a video lands, instead of waiting
for the tick. Thirty minutes becomes roughly one. See `transcode_wakeup.go`.

```
GITHUB_WORKER_TOKEN   a fine-grained personal access token, scoped to THIS
                      repository only, with Actions: read and write, and
                      nothing else at all
```

Set it wherever the backend runs. Nothing else to configure — the repo,
workflow and branch all have working defaults. Leave it unset and the code
does nothing, exactly as it does today.

Do not give this token any other permission, and do not use a classic token
that carries your whole account. Its one job is to press start on one
workflow.

**How to tell it is working.** Post a video and watch the Actions tab. A run
should appear within seconds, marked as manually triggered rather than
scheduled. If nothing appears, the backend logs say why — it names the actual
problem (token rejected, repo not found), never a bare status code.

### 2. Speech-to-text ⚠ TURN ON WHEN THERE IS HARDWARE FOR IT

The worker already looks at every video — how fast it cuts, how loud it is,
how much of it is silence, and the words a creator burns onto the screen. What
it does not do is listen. Turning what people SAY into text is the single
biggest remaining improvement to how well this app understands its own
content, and it is two environment variables away.

**Use whisper.cpp. Not the Python one.** Two different programs are both
called "whisper": OpenAI's original in Python, and `whisper.cpp`, a C++
rewrite. **This code talks to whisper.cpp** — it passes `-m`, `-f`, `-nt`,
`-np`, `-l`, which is whisper.cpp's command line. Hand it the Python one and
every video fails silently, which looks exactly like the feature being off.
whisper.cpp is also the right choice on its own merits here: no Python
runtime, one small binary, far less memory, and much faster on plain CPU.

```
WHISPER_BIN     path to the whisper.cpp binary  (e.g. /usr/local/bin/whisper-cli)
WHISPER_MODEL   path to a model file            (e.g. /models/ggml-base.en.bin)
```

Set both, make sure the binary and model are present wherever the worker runs,
and the speech pass starts on the next video. If either is unset the pass is
skipped silently and everything else works exactly as it does today — see
`cmd/hls-worker/analyze.go`.

**What "enough hardware" means here.** whisper.cpp with the `base.en` model is
about 150MB and transcribes a 30-second clip in a few seconds on a couple of
modern CPU cores. The `tiny.en` model is about 75MB and roughly twice as fast
with noticeably worse accuracy. Neither needs a GPU. So the bar is not high —
it is a few hundred megabytes of disk and a few CPU-seconds per upload.

**The real cost is throughput, not hardware.** Listening competes for the same
cores as transcoding, inside the same 24-minute budget, so fewer videos finish
per run. Invisible at low upload volume. The first thing to switch back off if
a backlog ever builds.

### Where the worker actually runs

This is the part that is easiest to get wrong. **Not on a hosting platform.**
It runs in GitHub Actions, from `.github/workflows/hls-worker.yml`, because
GitHub-hosted runners are free for public repositories and an always-on
background worker is not.

`cmd/hls-worker/Dockerfile` describes the container version and is **NOT** what
production uses. **Adding a tool to the Dockerfile alone changes nothing.**
Install it in the workflow.

To enable speech there: add a step that fetches a whisper.cpp build and a
model, cache them with `actions/cache` so they are not re-downloaded every
run, and set the two variables in the `drain` step's `env:` block. The runners
have 4 cores and the job already budgets 24 minutes, so there is room.

**How to tell which passes are running.** The worker records them with each
video, in `challenges.video_analysis` under `passes`. `["shape","text"]` means
it looked and read but did not listen. `["shape","text","speech"]` is all
three.

---

## What this thing actually does

Battle Arena is a short-video app built around challenges. Someone posts a
video with a prompt — *"Who is better — Dancer?"* — someone else posts a
response video, and viewers vote between them. A challenge nobody has answered
yet just plays in the feed as an ordinary short.

Most of the code here is the **feed**: deciding which video a given person sees
next. That is deliberately not "sort by popularity". The product goal is a feed
that leaves someone feeling better after twenty minutes rather than worse, and
a lot of the ranking exists to serve that rather than raw watch time.

---

## Running it

You need Go 1.25 and a PostgreSQL database. Redis is strongly recommended —
without it, session state and most of the learned ranking signals are disabled,
and the feed falls back to much simpler behaviour.

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/battlearena?sslmode=disable"
export JWT_SECRET="any-long-random-string-for-local-dev"
export REDIS_URL="redis://localhost:6379"

go run .
```

It listens on `:8081` unless `PORT` says otherwise. The schema is created on
first boot, and sample content is seeded if the tables are empty.

Health check: `GET /health`.

### Tests

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...     # what CI runs; needs a C compiler
```

No database or Redis needed — tests use `miniredis` and `sqlmock` throughout.

---

## Configuration

Everything is environment variables. Only the first two are required.

### Required

| Variable | What it does |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string. The service refuses to start without it. |
| `JWT_SECRET` | Signing key for session tokens. Without it login returns 500 and every authenticated route returns 401. Boot logs a warning rather than exiting, so tooling that never logs in still works. |

### Strongly recommended

| Variable | What it does |
|---|---|
| `REDIS_URL` (or `VALKEY_URL`) | Session state, rate-limit buckets, the seen-filter, and most learned ranking signals. The app runs without it, with a much simpler feed. |
| `ADMIN_USER`, `ADMIN_PASS` | Credentials for `/admin*`. **If either is unset every admin route answers 503** — that is the safe default, not an error. See the security note below before setting them. |

### Media and video

| Variable | What it does |
|---|---|
| `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | Cloudflare R2 credentials. Uploads are signed here and sent by the phone straight to R2 — video bytes never pass through this server. |
| `R2_PUBLIC_BASE_URL` | The public host clients fetch media from. |
| `HLS_WORKER_TOKEN` | Shared secret for the transcode worker's three internal endpoints. Must match the worker's copy. |

### Optional

| Variable | What it does |
|---|---|
| `PORT` | Listen port. Default `8081`. |
| `ALLOWED_ORIGINS` | Comma-separated list of web origins allowed to call this API from a browser, e.g. `https://app.example.com,https://staging.example.com`. **Unset keeps the historical `*` wildcard.** Native mobile clients never send an `Origin` header and are unaffected either way. |
| `MEILISEARCH_URL`, `MEILI_MASTER_KEY` | Full-text search. Search degrades gracefully when absent. |
| `FCM_SERVICE_ACCOUNT_JSON`, `FCM_PROJECT` | Push notifications via FCM HTTP v1. Raw or base64-encoded service-account JSON. |
| `MULTI_REPLICA` | Set to `1` when running more than one instance. Switches rate limiting to a shared Redis token bucket and turns on cross-replica WebSocket delivery. |

---

## Security notes worth reading before you deploy

**Rotate `ADMIN_PASS`.** The admin surface reads user analytics and operational
internals, and `/admin/reseed` **drops and rebuilds the database**. Guessing
that password is not a read-only mistake. Boot logs a warning if the value is a
common one or is shorter than 12 characters.

**Set `ALLOWED_ORIGINS` before shipping a web build.** The default wildcard has
been survivable because this API authenticates with a bearer token the client
attaches deliberately, not a cookie the browser attaches automatically — so a
random page can ask but learns nothing without the user's token. The day any
endpoint trusts a cookie, that stops being true.

**`JWT_SECRET` must be long and random**, and changing it logs everyone out —
tokens are stateless, so there is no revocation list, and a rotation
invalidates every session at once. Tokens last 7 days.

---

## How it is laid out

One flat Go package. Roughly 150 files at the top level, grouped by what they
do rather than by folder.

### The feed

| File | What it does |
|---|---|
| `feed_engine.go` | The core. Event ingest, session state, user profiles, content scoring, slot-based composition, and the main `/feed/smart` handler. Large. |
| `candidate_sources.go` | Five retrievers run in parallel (recency, trending, follow-graph, collaborative, embedding-neighbours) and merge by weight. Any one can fail without taking the feed down. |
| `learning_to_rank.go`, `bayesian_ltr.go` | Online model that learns a correction on top of the hand-tuned score, per cohort. |
| `calibration.go` | Turns the model's raw output into an actual probability so it can be combined with the other terms sanely. |
| `embeddings.go`, `trained_two_tower.go`, `pgvector_ann.go` | Content and user vectors, and similarity retrieval. |
| `seen_filter.go` | Remembers what you were shown for 12 hours so the feed does not repeat itself. |
| `mmr.go` | Breaks up near-duplicates in the top of the ranking. |
| `anti_loop.go`, `bandit.go` | Detect a session going badly and pick a different strategy. |
| `cohort.go`, `experiments.go` | Who this user is like, and A/B assignment. |
| `feed_kind_spacing.go` | Spaces battles and shorts through a page instead of clumping them. |
| `explore_feed.go` | The deliberately non-personalised discovery feed. |

### Everything else

| File | What it does |
|---|---|
| `main.go` | Routes, CORS, the global per-IP rate limiter, startup, graceful shutdown. |
| `auth.go`, `signup.go`, `password.go`, `totp.go` | Sessions, registration, bcrypt, two-factor. |
| `admin_auth.go`, `admin_*.go` | The admin dashboard and its Basic-Auth gate. |
| `action_limits.go` | Per-user, per-action rate limits (follow, comment, upload, login…). |
| `database.go` | Connection pool, baseline schema, queries. Large. |
| `schema_migrations.go` | Versioned run-once migrations. See `migrations/README.md`. |
| `media_storage.go`, `media_handlers.go`, `media_multipart.go` | R2 upload signing (hand-rolled SigV4). |
| `hls_worker_api.go` | The transcode queue's three internal endpoints. |
| `challenge_handler.go`, `challenge_validation.go` | Creating, answering, voting on challenges. |
| `chat_handler.go`, `websocket.go` | Direct messages and realtime delivery. |
| `search.go`, `search_ctr.go`, `meilisearch.go` | Search, and reranking it by its own click-through. |
| `notification_*.go`, `fcm_v1.go` | Push and in-app notifications. |
| `metrics.go` | Prometheus series. |

### Sub-commands

| Path | What it is |
|---|---|
| `cmd/hls-worker/` | The FFmpeg transcode worker. Runs as a GitHub Actions cron job every 30 minutes, which makes the transcode fleet cost nothing. |
| `cmd/seed/` | Replaces feed content with known sample reels. |
| `cmd/mediaimport/` | Imports MP4s into the bucket and the catalogue. |
| `smoketest/`, `loadtest/` | Black-box checks against a deployed instance. |
| `monitoring/` | Prometheus, Grafana and Alertmanager configuration. |

---

## Changing the database

Add a numbered `.sql` file to `migrations/`. It runs once, in order, at the
next boot, and is recorded so it never runs twice. **Read `migrations/README.md`
before writing one** — in particular, never edit a migration that has already
been deployed.

---

## Deployment

Runs on Render's free tier, which sleeps after about 15 minutes of inactivity
and takes 30–60 seconds to wake. That cold start is normal and the client is
built to expect it: a 30-second request timeout, and a login screen that says
"could not reach the server" rather than "wrong password" when a request times
out.

`.github/workflows/keepalive.yml` pings the service to reduce how often this
happens.
