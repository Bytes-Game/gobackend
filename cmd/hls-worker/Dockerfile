# syntax=docker/dockerfile:1.6
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

# ffmpeg     — the transcoder; ca-certificates  — needed for HTTPS GETs
# against R2 / the backend; tini       — proper PID 1 so signals from
# Render (SIGTERM on redeploy) reach the worker and let it exit cleanly
# mid-poll instead of being SIGKILLed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ffmpeg \
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
