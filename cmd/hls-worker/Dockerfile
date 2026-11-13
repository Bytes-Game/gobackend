# syntax=docker/dockerfile:1.6
#
# The video worker, always on: one container that waits for new videos and
# handles each the moment it is posted.
#
# What it does to each video, the same as the GitHub job
# (.github/workflows/hls-worker.yml) does today: makes the streaming
# versions, reads the words on screen (tesseract), listens to what is said
# (whisper.cpp), and works out what the video is about (llama.cpp with the
# Qwen3-VL model). Everything runs inside the container; no outside AI
# service, nothing to pay per video.
#
# The GitHub job starts from nothing on a schedule, or when a video is
# posted, so each run first spends a minute or more setting up. This
# container sets up once, then polls the server every few seconds for
# work, for as long as it runs.
#
# ────────────────────────────────────────────────────────────────────────
# KEEP IN STEP WITH THE GITHUB JOB
#
# The versions below (WHISPER_RELEASE, LLAMA_RELEASE, the model files) are
# the job's. Change one, change both.
#
# ────────────────────────────────────────────────────────────────────────
# WHAT IT NEEDS
#
# Memory: at least 8 GB. The understanding model takes about 4 GB while it
# runs, the speech model about 2 GB, ffmpeg the rest. With less, the system
# kills a model mid-video and the video gets no description, quietly.
# CPU: 4 cores is comfortable; each video then takes one to two minutes.
# Disk: the image is about 5.5 GB: 1 GB of programs, the rest the three
# model files.
#
# Settings (environment variables), the same as the GitHub job's secrets:
#   BACKEND_URL           the server, e.g. https://api.flexete.com
#   HLS_WORKER_TOKEN      the same value as the server's
#   R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
#   R2_PUBLIC_BASE_URL    optional; the server sends its own with each job
#
# Run ONE of these, or turn the GitHub job's schedule off once it runs:
# two workers do not share out a video, but each costs its own machine.
#
# ────────────────────────────────────────────────────────────────────────
# BUILD AND RUN
#
#   docker build -f cmd/hls-worker/Dockerfile -t flexete-worker .
#   docker run -d --restart unless-stopped --env-file worker.env flexete-worker
#
# from the repository's root. On a container host (Render "Background
# Worker", Railway, Fly.io, a VPS) point it at this file with the
# repository root as the build context.

ARG WHISPER_RELEASE=v1.9.3
ARG WHISPER_MODEL_NAME=medium
ARG LLAMA_RELEASE=b10819
ARG UNDERSTAND_REPO=Qwen/Qwen3-VL-4B-Instruct-GGUF
ARG UNDERSTAND_MODEL_FILE=Qwen3VL-4B-Instruct-Q4_K_M.gguf
ARG UNDERSTAND_PROJECTOR_FILE=mmproj-Qwen3VL-4B-Instruct-Q8_0.gguf

# ────────────── the worker itself ──────────────
FROM golang:1.27-bookworm AS worker

WORKDIR /src
# Module downloads in their own layer: go.sum changes far less often than
# the code, so rebuilds stay fast.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# No C libraries needed, so the binary runs on the slim image below.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/hls-worker ./cmd/hls-worker

# ────────────── whisper.cpp and llama.cpp ──────────────
FROM debian:bookworm AS engines
ARG WHISPER_RELEASE
ARG LLAMA_RELEASE

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential cmake git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# GGML_NATIVE=OFF: built for any x86-64 machine with AVX2 (every cloud
# server of the last ten years), not just for the one that builds it.
# Built "native", the image can crash with "illegal instruction" the
# first time it runs on a different processor.
ARG CPU_FLAGS="-DGGML_NATIVE=OFF -DGGML_AVX=ON -DGGML_AVX2=ON -DGGML_FMA=ON -DGGML_F16C=ON"

# whisper.cpp — the speech model's program. Its libraries are found next
# to the program (RPATH $ORIGIN), as in the GitHub job.
RUN git clone --depth 1 --branch "${WHISPER_RELEASE}" \
      https://github.com/ggml-org/whisper.cpp.git /tmp/whisper-src \
 && cmake -S /tmp/whisper-src -B /tmp/whisper-build \
      -DCMAKE_BUILD_TYPE=Release \
      -DWHISPER_BUILD_TESTS=OFF \
      -DWHISPER_BUILD_SERVER=OFF \
      -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON \
      -DCMAKE_INSTALL_RPATH='$ORIGIN' \
      ${CPU_FLAGS} \
 && cmake --build /tmp/whisper-build -j 4 --config Release \
 && mkdir -p /opt/whisper \
 && cp -r /tmp/whisper-build/bin /opt/whisper/bin \
 && rm -rf /tmp/whisper-build /tmp/whisper-src

# llama.cpp — runs the understanding model. Built into the programs
# (BUILD_SHARED_LIBS=OFF). Do not turn LLAMA_BUILD_SERVER off: llama-cli
# is built only with it on (see the GitHub job for the whole story).
RUN git clone --depth 1 --branch "${LLAMA_RELEASE}" \
      https://github.com/ggml-org/llama.cpp.git /tmp/llama-src \
 && cmake -S /tmp/llama-src -B /tmp/llama-build \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHARED_LIBS=OFF \
      -DLLAMA_CURL=OFF \
      -DLLAMA_BUILD_TESTS=OFF \
      ${CPU_FLAGS} \
 && cmake --build /tmp/llama-build --config Release -j 4 \
      --target llama-cli llama-mtmd-cli \
 && mkdir -p /opt/llama \
 && cp /tmp/llama-build/bin/llama-cli /tmp/llama-build/bin/llama-mtmd-cli /opt/llama/ \
 && rm -rf /tmp/llama-build /tmp/llama-src

# ────────────── the three model files ──────────────
FROM debian:bookworm-slim AS models
ARG WHISPER_MODEL_NAME
ARG UNDERSTAND_REPO
ARG UNDERSTAND_MODEL_FILE
ARG UNDERSTAND_PROJECTOR_FILE

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/models \
 && curl -fsSL --retry 3 -o /opt/models/whisper.bin \
      "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-${WHISPER_MODEL_NAME}.bin" \
 && curl -fsSL --retry 3 -o /opt/models/understand.gguf \
      "https://huggingface.co/${UNDERSTAND_REPO}/resolve/main/${UNDERSTAND_MODEL_FILE}" \
 && curl -fsSL --retry 3 -o /opt/models/projector.gguf \
      "https://huggingface.co/${UNDERSTAND_REPO}/resolve/main/${UNDERSTAND_PROJECTOR_FILE}" \
 && test -s /opt/models/whisper.bin \
 && test -s /opt/models/understand.gguf \
 && test -s /opt/models/projector.gguf

# ────────────── what runs ──────────────
FROM debian:bookworm-slim AS runtime

# ffmpeg     the streaming versions, and the measuring pass (analyze.go)
# tesseract  reads words burned into the picture; the eng data is needed
# libgomp1   lets whisper.cpp and llama.cpp use every core
# tini       passes the stop signal on, so a redeploy stops the worker
#            cleanly between jobs instead of killing it mid-video
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ffmpeg tesseract-ocr tesseract-ocr-eng ca-certificates libgomp1 tini \
 && rm -rf /var/lib/apt/lists/*

COPY --from=engines /opt/whisper /opt/whisper
COPY --from=engines /opt/llama /opt/llama
COPY --from=models /opt/models /opt/models
COPY --from=worker /out/hls-worker /usr/local/bin/hls-worker

# Where the worker finds the three passes: the same names the GitHub job
# sets. One that is missing is skipped, and the video says so in the
# passes it lists.
ENV WHISPER_BIN=/opt/whisper/bin/whisper-cli \
    WHISPER_MODEL=/opt/models/whisper.bin \
    LD_LIBRARY_PATH=/opt/whisper/bin \
    UNDERSTAND_BIN=/opt/llama/llama-cli \
    UNDERSTAND_MODEL=/opt/models/understand.gguf \
    UNDERSTAND_FRAMES_BIN=/opt/llama/llama-mtmd-cli \
    UNDERSTAND_PROJECTOR=/opt/models/projector.gguf

# Not as root: it only reads and writes its own temporary files and makes
# outgoing connections.
RUN useradd --system --create-home --shell /usr/sbin/nologin worker
USER worker
WORKDIR /home/worker

ENTRYPOINT ["/usr/bin/tini", "--"]
# Polls for new videos every 5 seconds, for ever. -job-timeout as in the
# GitHub job: a video that takes longer than 16 minutes is given up on and
# tried again later, instead of holding the worker.
CMD ["/usr/local/bin/hls-worker", "-poll", "5s", "-job-timeout", "16m"]
