# syntax=docker/dockerfile:1.6
#
# ⚠ THIS FILE IS NOT WHAT RUNS IN PRODUCTION. ⚠
#
# The transcode worker runs in GitHub Actions, on a schedule, from
# .github/workflows/hls-worker.yml. That is where tools get installed and
# where environment variables get set. Changing this Dockerfile alone
# changes nothing about the live app. Keep the two in step, but treat the
# workflow as the real one.
#
# This file is the always-on container version, kept working for the day
# somebody wants to pay for a background worker instead of running the
# queue down every 30 minutes for free.
#
# ────────────────────────────────────────────────────────────────────────
#
# Dockerfile for the HLS transcode worker (sibling binary of the main
# devb backend). Designed to deploy as a Render Background Worker but
# works on any container host with outbound HTTPS to the backend +
# Cloudflare R2.
#
# Two-stage build:
#   1. `builder` — full golang image, compiles ./cmd/hls-worker into a
#      static-ish binary. We build with CGO disabled so the final image
#      doesn't need glibc; that lets us land on the smaller debian-slim
#      runtime even though Go itself doesn't strictly need musl.
#   2. `runtime` — debian-slim with ffmpeg + ca-certificates only. Ships
#      around ~250MB, dominated by ffmpeg's codec libraries — there's no
#      smaller-but-still-correct way to get x264 + AAC encoding.
#
# Why debian-slim and not alpine: alpine's ffmpeg package omits some
# codec libraries we want (notably libfdk_aac, though we use the built-
# in `aac` encoder so it's not strictly required) and uses musl, which
# has tripped us up with DNS resolution on some hosting providers in
# the past. The size difference (~250MB vs ~180MB) is not worth the
# debugging time.
#
# Build context expectation: build from the devb/ module root so the
# go.mod / go.sum / vendor-or-not all resolve correctly:
#
#     docker build -f cmd/hls-worker/Dockerfile -t devf-hls-worker .
#
# Or on Render: set the Dockerfile path to `cmd/hls-worker/Dockerfile`
# and the root directory to `devb/`.

# ────────────── builder stage ──────────────
FROM golang:1.23-bookworm AS builder

WORKDIR /src

# Cache module downloads in their own layer. go.sum changes much less
# often than the source, so this keeps incremental builds fast.
COPY go.mod go.sum ./
RUN go mod download

# Now copy the rest of the module (everything we need to compile the
# worker binary lives in the same Go module as the backend, even though
# we only build cmd/hls-worker here).
COPY . .

# Static-ish build: CGO_ENABLED=0 produces a binary with no libc deps,
# so it runs on any glibc-or-musl runtime. -trimpath strips local paths
# from stack traces (smaller binary, no leaked $HOME). -ldflags "-s -w"
# removes debug symbols (another ~30% size reduction).
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/hls-worker \
    ./cmd/hls-worker

# ────────────── runtime stage ──────────────
FROM debian:bookworm-slim AS runtime

# ffmpeg     — the transcoder, and now also the measuring tool: the worker
#              runs one extra pass over each source to work out how fast it
#              cuts, how loud it is and how much of it is silence. See
#              analyze.go. No extra package needed for that part.
# tesseract  — reads words burned into the picture. Adds roughly 40MB, and
#              earns it: on short video the caption a creator types onto the
#              screen is very often the single best description of what the
#              video is, and it is invisible to everything else. The eng
#              language data is a separate package and is required — the
#              binary alone cannot read anything.
# ca-certificates — needed for HTTPS GETs against R2 / the backend
# tini       — proper PID 1 so signals from Render (SIGTERM on redeploy)
#              reach the worker and let it exit cleanly mid-poll instead of
#              being SIGKILLed.
#
# Speech-to-text is deliberately NOT installed here. It wants a few hundred
# megabytes of model and real CPU time per video, which is a decision about
# hosting cost rather than about code — so the worker looks for a whisper
# binary and model at run time via WHISPER_BIN and WHISPER_MODEL, and simply
# skips the pass when they are not set. To turn it on, add whisper.cpp and a
# model to this image and set both variables.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ffmpeg \
        tesseract-ocr \
        tesseract-ocr-eng \
        ca-certificates \
        tini \
 && rm -rf /var/lib/apt/lists/*

# Run as a non-root user. The worker only needs to read/write inside
# its own tmp dir and make outbound HTTP calls — there's no reason to
# give it the rest of the container.
RUN useradd --system --create-home --shell /usr/sbin/nologin worker
USER worker
WORKDIR /home/worker

COPY --from=builder /out/hls-worker /usr/local/bin/hls-worker

# tini handles signal forwarding so SIGTERM from the orchestrator
# actually reaches our Go process. Without it, you get 30s shutdown
# delays on every Render redeploy.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/hls-worker"]
