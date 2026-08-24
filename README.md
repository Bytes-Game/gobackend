# gobackend — Battle Arena API

The Go server behind the Battle Arena app. It serves the feed, the challenge
and battle system, chat, search, notifications, and media upload signing.

The Flutter client lives in a separate repo (`Frontend`).

---

## ⚠ READ THIS BEFORE GOING LIVE

**If you are a person or a model picking this repo up, start here.**

One thing is written, tested and switched OFF, and it **must** be dealt with
before real users arrive. It is off because switching it on is somebody's
decision to make, not the code's.

| | what | before launch? |
|---|---|---|
| 1 | uploads can wait up to 30 minutes to become playable | **YES — one secret to set** |
| 2 | speech-to-text: running, but on the smallest model | no — upgrade when you want |

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

Nothing else to configure — the repo, workflow and branch all have working
defaults. Leave it unset and the code does nothing, exactly as it does today.

**Making the token** — on github.com, signed in as an account that can push to
this repo:

1. Profile picture → **Settings** (your account's, not the repo's)
2. Bottom of the left menu → **Developer settings**
3. **Personal access tokens** → **Fine-grained tokens** → **Generate new token**
4. **Repository access** → *Only select repositories* → pick **this one**
5. **Permissions** → *Repository permissions* → find **Actions** → set to
   **Read and write**. Leave every other permission alone.
6. Generate, and copy the token — GitHub shows it once and never again.

**Installing it** — in the Render dashboard, on the backend service:
**Environment** → **Add Environment Variable** → name `GITHUB_WORKER_TOKEN`,
value the token → **Save**. Render restarts the service and it is live.

Step 4 is the one that matters. A fine-grained token limited to one repository
with one permission can start this workflow and do **nothing else anywhere**.
A classic token carries your whole account, so never use one here.

Fine-grained tokens expire — a year at most, and the default is shorter. When
it does, uploads quietly go back to waiting for the timer and the backend log
starts saying the token was refused. Set a calendar reminder, and note the
expiry date somewhere you will look.

**How to tell it is working.** Post a video and watch the Actions tab. A run
should appear within seconds, marked as manually triggered rather than
scheduled. If nothing appears, the backend logs say why — it names the actual
problem (token rejected, repo not found), never a bare status code.

**How to tell it is working.** Post a video and watch the Actions tab. A run
should appear within seconds, marked as manually triggered rather than
scheduled. If nothing appears, the backend logs say why — it names the actual
problem (token rejected, repo not found), never a bare status code.

### 2. Speech-to-text — ON, at the smallest useful size

The worker hears every video as well as looking at it. `.github/workflows/
hls-worker.yml` builds whisper.cpp, caches it, and points the worker at it.
Nothing to set.

**To make transcripts better, move UP a model size. That is the whole upgrade
path.** One line in the workflow:

```yaml
WHISPER_MODEL_NAME: base.en
```

|  | size | speed | quality |
|---|---|---|---|
| `tiny.en`   | 75MB  | fastest      | rough |
| `base.en`   | 142MB | **current**  | good enough for tags and categories |
| `small.en`  | 466MB | ~3× slower   | clearly better |
| `medium.en` | 1.5GB | ~8× slower   | better still |

Change the line, and the next run rebuilds the cache once and then reuses it.
For languages other than English, drop the `.en` (e.g. `small`) — the
English-only models ignore the language setting.

> ### ⚠ DO NOT "UPGRADE" TO THE PYTHON WHISPER
>
> There are two different programs called "whisper": OpenAI's original in
> Python, and `whisper.cpp`, a C++ rewrite. **They run the same models and
> produce the same transcripts.** The Python one is not a better version, a
> newer version, or a production version — it is the same thing in a different
> language, and here it is strictly worse:
>
> - **It would break this code.** The worker passes `-m`, `-f`, `-nt`, `-np`,
>   `-l` — whisper.cpp's command line. The Python one takes different
>   arguments, so every video would fail. **Silently**, looking exactly like
>   the feature being switched off.
> - It needs Python and PyTorch — gigabytes, versus one small binary.
> - It is far slower without a graphics card, and the runners do not have one.
>
> If somebody wrote down "switch to Python when we go live", that note was
> based on a misunderstanding. **Upgrade the model, not the program.**

**The real cost is throughput, not hardware.** Listening competes for the same
cores as transcoding, inside the same 24-minute budget, so fewer videos finish
per run. Invisible at low upload volume. The first thing to switch back off if
a backlog ever builds — delete the `WHISPER_*` lines from the workflow and the
worker goes back to looking without listening, with nothing else affected.

**If the build or the model download ever fails, the run still transcodes.**
The check step is `continue-on-error`, and the worker skips speech when the
binary is not where it was told to look. Losing transcripts costs some feed
quality; losing the run costs every creator their upload.

### Where the worker actually runs

This is the part that is easiest to get wrong. **Not on a hosting platform.**
It runs in GitHub Actions, from `.github/workflows/hls-worker.yml`, because
GitHub-hosted runners are free for public repositories and an always-on
background worker is not.

`cmd/hls-worker/Dockerfile` describes the container version and is **NOT** what
production uses. **Adding a tool to the Dockerfile alone changes nothing.**
Install it in the workflow.

**How to tell which passes are running.** The worker records them with each
video, in `challenges.video_analysis` under `passes`:

| `passes` | meaning |
|---|---|
| `["shape","text","speech"]` | all three — what a healthy run looks like |
| `["shape","text"]` | whisper did not load; check the "check whisper works" step |
| `["shape"]` | no words found, or tesseract missing |
| `[]` or absent | nothing was measured — see below |

An **empty or missing** reading is not "a silent, still, dark video". It means
nobody looked. Everything downstream is written to treat those two as
different, and must stay that way: a video with no reading that got scored as
if every measurement were zero would be pushed into the same corner of the
feed as genuinely dull content, for no reason but having been uploaded before
the worker got to it.

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
