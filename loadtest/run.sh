#!/usr/bin/env bash
# Runs one load-test scenario against a fresh docker compose stack and collects the results.
#
#   loadtest/run.sh <workers> <name> [env...]
#
# Env: RATE (jobs/s, default 500)  DURATION (default 30s)  KILL_AT (seconds into the test at which
# to SIGKILL one worker; unset = no kill)  WORKER_LEASE_DURATION / WORKER_REAPER_INTERVAL (compose).
set -euo pipefail

WORKERS=${1:?usage: run.sh <workers> <name>}
NAME=${2:?usage: run.sh <workers> <name>}
export RATE=${RATE:-500} DURATION=${DURATION:-30s} REPORT_RATE=${REPORT_RATE:-0} REPORT_SLEEP_MS=${REPORT_SLEEP_MS:-2000}
export API_PORT=${API_PORT:-18080} POSTGRES_PORT=${POSTGRES_PORT:-55432}
export PROMETHEUS_PORT=${PROMETHEUS_PORT:-19090} GRAFANA_PORT=${GRAFANA_PORT:-13000}

cd "$(dirname "$0")/.."
psql_() { docker compose exec -T postgres psql -U jobqueue -tAc "$1"; }

echo "== [$NAME] fresh stack with $WORKERS worker(s)"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build --scale worker="$WORKERS" >/dev/null 2>&1
for _ in $(seq 60); do curl -sf "localhost:$API_PORT/actuator/health" >/dev/null && break; sleep 2; done
curl -sf "localhost:$API_PORT/actuator/health" >/dev/null || { echo "API did not become healthy"; exit 1; }
sleep 5 # let every worker finish starting

echo "== [$NAME] k6: $RATE jobs/s for $DURATION"
START=$(date +%s)
docker run --rm -v "$PWD/loadtest:/loadtest" \
  -e BASE_URL="http://host.docker.internal:$API_PORT" -e RATE -e DURATION -e NAME="$NAME" \
  -e REPORT_RATE -e REPORT_SLEEP_MS \
  grafana/k6:latest run --quiet /loadtest/k6/enqueue.js > "loadtest/results/$NAME-k6.txt" 2>&1 &
K6=$!

if [ -n "${KILL_AT:-}" ]; then
  sleep "$KILL_AT"
  VICTIM=$(docker ps --filter "label=com.docker.compose.service=worker" --format '{{.Names}}' | head -1)
  HELD=$(psql_ "select count(*) from jobs where status='RUNNING' and locked_by like '$(docker inspect --format '{{.Config.Hostname}}' "$VICTIM")%'")
  echo "== [$NAME] t+${KILL_AT}s: SIGKILL $VICTIM (holding $HELD running jobs)"
  echo "$HELD" > "loadtest/results/$NAME-held.txt"
  docker kill "$VICTIM" >/dev/null
  echo "$VICTIM" > "loadtest/results/$NAME-victim.txt"
fi
wait $K6 || true
ENQ_DONE=$(date +%s)

if [ "$WORKERS" = "0" ]; then
  # Enqueue-only run (no workers): nothing drains, so just check every accepted job was stored.
  python3 loadtest/collect_enqueue_only.py "$NAME" "$(psql_ "select count(*) from jobs")"
  exit 0
fi
echo "== [$NAME] enqueue finished after $((ENQ_DONE-START))s; waiting for the queue to drain"
for _ in $(seq 600); do
  [ "$(psql_ "select count(*) from jobs where status in ('PENDING','RUNNING')")" = "0" ] && break
  sleep 2
done

python3 loadtest/collect.py "$NAME" "$WORKERS" "${KILL_AT:-}" \
  "$(psql_ "$(cat loadtest/collect.sql)")"
