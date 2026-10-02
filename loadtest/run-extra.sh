#!/usr/bin/env bash
# Follow-up experiments after the first matrix showed (a) a latency tail at low load that looked like
# idle-poll backoff and (b) kill runs that sometimes killed a worker holding no jobs.
set -euo pipefail
cd "$(dirname "$0")/.."
for r in 1 2 3; do
  # (a) same steady load as the matrix, but idle backoff capped at 500 ms instead of 5 s
  WORKER_MAX_POLL_INTERVAL=500ms RATE=100 DURATION=45s loadtest/run.sh 1 steadyfast-w1-r$r
  # (b) crash recovery with a guaranteed in-flight population: slow jobs keep slots busy
  WORKER_LEASE_DURATION=10s WORKER_REAPER_INTERVAL=2s RATE=100 REPORT_RATE=12 REPORT_SLEEP_MS=2000 \
    DURATION=60s KILL_AT=25 loadtest/run.sh 4 kill-w4-r$r
  # (c) API-only enqueue capacity: no workers competing for CPU or the database
  RATE=1500 DURATION=20s loadtest/run.sh 0 enqonly-w0-r$r
done
docker compose down -v >/dev/null 2>&1 || true
