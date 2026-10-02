#!/usr/bin/env bash
# The full benchmark matrix used for the README results. Each scenario is repeated (default 3x) because
# single runs on a laptop vary; the README reports medians and ranges.
set -euo pipefail
cd "$(dirname "$0")/.."
REPS=${REPS:-"1 2 3"}
for r in $REPS; do
  # 1) Saturating burst: offered load above what 1-2 workers can drain, with enqueue and processing
  #    competing for the same Postgres.
  for w in 1 2 4; do RATE=500 DURATION=30s loadtest/run.sh $w burst-w$w-r$r; done
  # 2) Steady load below single-worker capacity: latency under normal operation.
  for w in 1 2 4; do RATE=100 DURATION=45s loadtest/run.sh $w steady-w$w-r$r; done
  # 3) Pure processing capacity: queue pre-filled in the database, no enqueue load.
  for w in 1 2 4; do loadtest/run-drain.sh $w drain-w$w-r$r 20000; done
  # 4) Crash during load: SIGKILL one of 4 workers 20 s in (10 s lease, 2 s reaper to keep it short).
  WORKER_LEASE_DURATION=10s WORKER_REAPER_INTERVAL=2s RATE=300 DURATION=45s KILL_AT=20 \
    loadtest/run.sh 4 kill-w4-r$r
done
docker compose down -v >/dev/null 2>&1 || true
