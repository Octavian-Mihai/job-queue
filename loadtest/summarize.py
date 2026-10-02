#!/usr/bin/env python3
"""Aggregate loadtest/results/<scenario>-w<N>-r<rep>.json into median [min-max] tables (markdown)."""
import glob
import json
import re
import statistics
from collections import defaultdict

runs = defaultdict(list)
for f in sorted(glob.glob("loadtest/results/*-w*-r*.json")):
    if f.endswith("-k6.json"):
        continue
    m = re.match(r".*/(burst|steady|steady5s|steadyfast|drain|kill|enqonly)-w(\d+)-r(\d+)\.json$", f)
    if m:
        runs[(m.group(1), int(m.group(2)))].append(json.load(open(f)))


def cell(rs, key, fmt="{:.0f}"):
    vals = [r[key] for r in rs]
    med = statistics.median(vals)
    if len(vals) == 1:
        return fmt.format(med)
    return f"{fmt.format(med)} [{fmt.format(min(vals))}-{fmt.format(max(vals))}]"


def table(title, scenario, cols, note=""):
    out = [f"**{title}**", ""]
    out.append("| Workers | Runs | " + " | ".join(c[0] for c in cols) + " |")
    out.append("|---|---|" + "|".join("---" for _ in cols) + "|")
    for (s, w), rs in sorted(runs.items(), key=lambda kv: kv[0][1]):
        if s != scenario:
            continue
        out.append(f"| {w} | {len(rs)} | " + " | ".join(cell(rs, k, f) for _, k, f in cols) + " |")
    if note:
        out += ["", note]
    return "\n".join(out)


parts = [
    table("Burst: 500 jobs/s offered for 30 s (enqueue and processing share one Postgres)", "burst", [
        ("enqueue/s (achieved)", "enqueue_throughput_per_s", "{:.0f}"),
        ("enqueue p95 (ms)", "enqueue_p95_ms", "{:.0f}"),
        ("dropped by k6", "dropped_iterations", "{:.0f}"),
        ("jobs/min processed", "jobs_per_min", "{:.0f}"),
        ("e2e p50 (s)", "e2e_p50_s", "{:.1f}"),
        ("e2e p95 (s)", "e2e_p95_s", "{:.1f}"),
    ]),
    table("Steady: 100 jobs/s for 45 s (default idle poll ceiling: 1 s)", "steady", [
        ("enqueue/s", "enqueue_throughput_per_s", "{:.0f}"),
        ("enqueue p95 (ms)", "enqueue_p95_ms", "{:.0f}"),
        ("e2e p50 (s)", "e2e_p50_s", "{:.2f}"),
        ("e2e p95 (s)", "e2e_p95_s", "{:.2f}"),
    ]),
    table("Steady, previous default idle poll ceiling of 5 s (same load)", "steady5s", [
        ("enqueue/s", "enqueue_throughput_per_s", "{:.0f}"),
        ("enqueue p95 (ms)", "enqueue_p95_ms", "{:.0f}"),
        ("e2e p50 (s)", "e2e_p50_s", "{:.2f}"),
        ("e2e p95 (s)", "e2e_p95_s", "{:.2f}"),
    ]),
    table("Steady, idle poll ceiling of 500 ms (same load)", "steadyfast", [
        ("enqueue/s", "enqueue_throughput_per_s", "{:.0f}"),
        ("e2e p50 (s)", "e2e_p50_s", "{:.2f}"),
        ("e2e p95 (s)", "e2e_p95_s", "{:.2f}"),
    ]),
    table("Enqueue only: API with no workers, 1,500 jobs/s offered for 20 s", "enqonly", [
        ("enqueue/s (achieved)", "enqueue_throughput_per_s", "{:.0f}"),
        ("enqueue p50 (ms)", "enqueue_p50_ms", "{:.1f}"),
        ("enqueue p95 (ms)", "enqueue_p95_ms", "{:.0f}"),
        ("enqueue p99 (ms)", "enqueue_p99_ms", "{:.0f}"),
        ("dropped by k6", "dropped_iterations", "{:.0f}"),
    ]),
    table("Drain: 20,000 jobs pre-filled, no enqueue load (pure processing capacity)", "drain", [
        ("window (s)", "window_s", "{:.0f}"),
        ("jobs/min", "jobs_per_min", "{:.0f}"),
        ("jobs/min per worker", "jobs_per_min_per_worker", "{:.0f}"),
    ]),
    table("Kill: 100 emails/s + 12 slow (2 s) reports/s for 60 s; one of 4 workers SIGKILLed at t+25 s (10 s lease)", "kill", [
        ("accepted (201)", "jobs_accepted_201", "{:.0f}"),
        ("succeeded", "succeeded", "{:.0f}"),
        ("lost", "lost_jobs", "{:.0f}"),
        ("jobs running when killed", "running_when_killed", "{:.0f}"),
        ("attempts reclaimed (LEASE_EXPIRED)", "attempts_lease_expired", "{:.0f}"),
        ("e2e p95 (s)", "e2e_p95_s", "{:.1f}"),
        ("e2e max (s)", "e2e_max_s", "{:.1f}"),
    ]),
]
failed = [
    f"{s}-w{w}" for (s, w), rs in runs.items() for r in rs if not r.get("verification_passed", False)
]
parts.append(
    f"Zero-lost-jobs verification: {sum(len(v) for v in runs.values())} runs, "
    f"{'ALL PASSED' if not failed else 'FAILED: ' + ', '.join(failed)}."
)
text = "\n\n".join(parts) + "\n"
open("loadtest/results/SUMMARY.md", "w").write(text)
print(text)
