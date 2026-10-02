#!/usr/bin/env python3
"""Merge the k6 summary with database measurements, check for lost jobs, write results/<name>.json."""
import json
import sys

name, workers, kill_at, db_json = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
db = json.loads(db_json)
k6 = json.load(open(f"loadtest/results/{name}-k6.json"))
m = k6["metrics"]


def metric(metric_name, field):
    return m.get(metric_name, {}).get("values", m.get(metric_name, {})).get(field, 0)


accepted = int(metric("jobs_accepted", "count"))
rejected = int(metric("jobs_rejected", "count"))
duration_s = k6["state"]["testRunDurationMs"] / 1000.0
terminal = db["succeeded"] + db["dead"] + db["cancelled"]
r = {
    "name": name,
    "workers": workers,
    "killed_a_worker_at_s": int(kill_at) if kill_at else None,
    # --- enqueue side (k6) ---
    "jobs_accepted_201": accepted,
    "jobs_rejected": rejected,
    "dropped_iterations": int(metric("dropped_iterations", "count")),
    "enqueue_window_s": round(duration_s, 1),
    "enqueue_throughput_per_s": round(accepted / duration_s, 1),
    "enqueue_p50_ms": round(metric("http_req_duration", "med"), 1),
    "enqueue_p95_ms": round(metric("http_req_duration", "p(95)"), 1),
    "enqueue_p99_ms": round(metric("http_req_duration", "p(99)"), 1),
    # --- processing side (database) ---
    "jobs_in_db": db["jobs_in_db"],
    "succeeded": db["succeeded"],
    "dead": db["dead"],
    "not_terminal": db["not_terminal"],
    "makespan_s": round(db["makespan_s"], 1),
    "jobs_per_min": round(db["succeeded"] / db["makespan_s"] * 60) if db["makespan_s"] else 0,
    "jobs_per_min_per_worker": round(db["succeeded"] / db["makespan_s"] * 60 / workers) if db["makespan_s"] else 0,
    "e2e_p50_s": round(db["e2e_p50_s"], 2),
    "e2e_p95_s": round(db["e2e_p95_s"], 2),
    "e2e_p99_s": round(db["e2e_p99_s"], 2),
    "e2e_max_s": round(db["e2e_max_s"], 2),
    # --- failure bookkeeping ---
    "attempts_total": db["attempts_total"],
    "attempts_lease_expired": db["attempts_lease_expired"],
    "jobs_with_more_than_one_attempt": db["jobs_with_more_than_one_attempt"],
    "succeeded_send_email": db["succeeded_send_email"],
    "emails_in_outbox": db["emails_in_outbox"],
    "dlq_entries": db["dlq_entries"],
    "distinct_workers_that_ran_jobs": db["distinct_workers"],
}
# --- the zero-lost-jobs verification ---
checks = {
    "every accepted (201) job exists in the database": accepted == db["jobs_in_db"],
    "every job reached a terminal state": terminal == db["jobs_in_db"] and db["not_terminal"] == 0,
    "enqueued count == terminal-state count": accepted == terminal,
    "no job dead-lettered": db["dead"] == 0,
    "each succeeded send-email job produced exactly one email (idempotent handler)": db["emails_in_outbox"] == db["succeeded_send_email"],
}
import os
held_file = f"loadtest/results/{name}-held.txt"
r["running_when_killed"] = int(open(held_file).read()) if os.path.exists(held_file) else None
r["lost_jobs"] = accepted - terminal
r["verification"] = checks
r["verification_passed"] = all(checks.values())
json.dump(r, open(f"loadtest/results/{name}.json", "w"), indent=2)

print(f"\n=== {name}: {workers} worker(s) ===")
for k in ("jobs_accepted_201", "dropped_iterations", "enqueue_throughput_per_s", "enqueue_p95_ms",
          "makespan_s", "jobs_per_min", "jobs_per_min_per_worker", "e2e_p50_s", "e2e_p95_s",
          "attempts_lease_expired", "jobs_with_more_than_one_attempt"):
    print(f"  {k:36} {r[k]}")
print(f"  lost jobs: {r['lost_jobs']}   verification: {'PASS' if r['verification_passed'] else 'FAIL'}")
for c, ok in checks.items():
    print(f"    [{'x' if ok else ' '}] {c}")
