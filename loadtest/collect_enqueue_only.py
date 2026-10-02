#!/usr/bin/env python3
"""Enqueue-only run (no workers): k6 numbers plus a check that every 201 was actually stored."""
import json
import sys

name, in_db = sys.argv[1], int(sys.argv[2])
m = json.load(open(f"loadtest/results/{name}-k6.json"))
k6 = m["metrics"]
dur = m["state"]["testRunDurationMs"] / 1000.0
accepted = int(k6["jobs_accepted"]["values"]["count"])
v = k6["http_req_duration"]["values"]
r = {
    "name": name, "workers": 0,
    "jobs_accepted_201": accepted, "jobs_rejected": int(k6.get("jobs_rejected", {}).get("values", {}).get("count", 0)),
    "dropped_iterations": int(k6.get("dropped_iterations", {}).get("values", {}).get("count", 0)),
    "enqueue_throughput_per_s": round(accepted / dur, 1),
    "enqueue_p50_ms": round(v["med"], 1), "enqueue_p95_ms": round(v["p(95)"], 1), "enqueue_p99_ms": round(v["p(99)"], 1),
    "jobs_in_db": in_db,
}
r["lost_jobs"] = accepted - in_db
r["verification_passed"] = accepted == in_db
json.dump(r, open(f"loadtest/results/{name}.json", "w"), indent=2)
print(json.dumps(r, indent=2))
