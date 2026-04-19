#!/usr/bin/env bash
# Runs the race-detector test suite inside a Linux Docker container so
# Windows developers (no gcc installed) can still exercise -race locally.
#
# Usage (from devb/):
#   bash scripts/race-in-docker.sh
#
# Requires: Docker Desktop (Windows/Mac) or docker engine (Linux).
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

docker run --rm \
  -v "$REPO_ROOT":/src \
  -w /src \
  golang:1.22 \
  bash -c "go mod download && go test -race -count=1 ./..."
