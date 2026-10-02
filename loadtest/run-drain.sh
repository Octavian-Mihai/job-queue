#!/usr/bin/env bash
# Pure processing capacity: pre-fill the queue straight into Postgres (no API, no k6) and time how
# long N workers take to drain it. Also samples container CPU mid-run to show what is saturated.
#
#   loadtest/run-drain.sh <workers> <name> [jobs]
set -euo pipefail
WORKERS=${1:?usage: run-drain.sh <workers> <name> [jobs]}
NAME=${2:?}
JOBS=${3:-20000}
export API_PORT=${API_PORT:-18080} POSTGRES_PORT=${POSTGRES_PORT:-55432}
export PROMETHEUS_PORT=${PROMETHEUS_PORT:-19090} GRAFANA_PORT=${GRAFANA_PORT:-13000}
cd "$(dirname "$0")/.."
psql_() { docker compose exec -T postgres psql -U jobqueue -tAc "$1"; }

echo "== [$NAME] fresh stack with $WORKERS worker(s), then pre-filling $JOBS jobs"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build --scale worker="$WORKERS" >/dev/null 2>&1
for _ in $(seq 60); do curl -sf "localhost:$API_PORT/actuator/health" >/dev/null && break; sleep 2; done
sleep 8 # workers idle and warm
psql_ "insert into jobs (type, payload) select 'send-email', '{\"to\":\"u@example.com\",\"simulate\":\"ok\"}'::jsonb from generate_series(1, $JOBS)" >/dev/null

sleep 15
docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' > "loadtest/results/$NAME-cpu.txt"
for _ in $(seq 900); do
  [ "$(psql_ "select count(*) from jobs where status in ('PENDING','RUNNING')")" = "0" ] && break
  sleep 2
done

python3 - "$NAME" "$WORKERS" "$(psql_ "select json_build_object(
  'jobs', count(*), 'succeeded', count(*) filter (where status='SUCCEEDED'),
  'dead', count(*) filter (where status='DEAD'),
  'window_s', (select extract(epoch from (max(finished_at) - min(started_at))) from job_attempts),
  'emails', (select count(*) from email_outbox),
  'distinct_workers', (select count(distinct worker_id) from job_attempts)) from jobs")" <<'PY'
import json, sys, re
name, workers, db = sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3])
cpu = {}
for line in open(f"loadtest/results/{name}-cpu.txt"):
    n, pct = line.split()
    svc = "postgres" if "postgres" in n and "shortener" not in n else "worker" if "worker" in n and "shortener" not in n else None
    if svc:
        cpu.setdefault(svc, []).append(float(pct.rstrip("%")))
r = {
    "name": name, "workers": workers, "jobs": db["jobs"], "succeeded": db["succeeded"], "dead": db["dead"],
    "window_s": round(db["window_s"], 1),
    "jobs_per_min": round(db["succeeded"] / db["window_s"] * 60),
    "jobs_per_min_per_worker": round(db["succeeded"] / db["window_s"] * 60 / workers),
    "emails_in_outbox": db["emails"], "distinct_workers": db["distinct_workers"],
    "cpu_percent_mid_run": {k: (round(sum(v), 1) if k == "worker" else round(v[0], 1)) for k, v in cpu.items()},
}
r["verification_passed"] = (db["succeeded"] == db["jobs"] and db["emails"] == db["succeeded"])
json.dump(r, open(f"loadtest/results/{name}.json", "w"), indent=2)
print(json.dumps(r, indent=2))
PY
