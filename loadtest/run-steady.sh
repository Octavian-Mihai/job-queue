#!/usr/bin/env bash
# Steady-load scenario at the default (1 s) idle poll ceiling, 3 repetitions per worker count.
set -euo pipefail
cd "$(dirname "$0")/.."
for r in 1 2 3; do
  for w in 1 2 4; do RATE=100 DURATION=45s loadtest/run.sh $w steady-w$w-r$r; done
done
docker compose down -v >/dev/null 2>&1 || true
