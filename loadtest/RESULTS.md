# Load test results

Reproduce: `loadtest/run-all.sh` (matrix, 3 repetitions each), `loadtest/run-extra.sh`,
`loadtest/run-steady.sh`, then `loadtest/summarize.py`. Raw per-run JSON, k6 output and CPU samples are in
[`results/`](results). k6 runs from the official `grafana/k6` image.

**Machine and caveats (read before quoting any number).** MacBook with an Apple M3 (8 cores), 16 GB
RAM, macOS 27.0, Docker Desktop (VM: 8 CPUs, 8 GB). Everything shares that one machine: k6, the API,
the workers, an untuned PostgreSQL 16 in a container, Prometheus and Grafana, plus an unrelated idle
container stack (under 3% CPU when sampled). So these are **relative** numbers, not capacity claims for
real hardware. The handlers are simulated (`send-email`: 20-80 ms of sleep and one idempotent insert, no
real CPU work), so per-worker throughput reflects queue overhead plus that latency, not business logic.
Each scenario ran **3 times**; cells show *median [min-max]*. Every run starts from a fresh stack and
database, so JVM warm-up is included. End-to-end latency is `finished_at - created_at` from the database
(one clock). Scenarios: **burst** offers 500 jobs/s for 30 s so 1-2 workers fall behind; **steady** offers
100 jobs/s for 45 s (below one worker's capacity); **drain** pre-fills 20,000 jobs directly in Postgres to
measure pure processing capacity with no enqueue load; **kill** is described below.

**Headline (median of 3 runs per cell; details and ranges below)**

| Workers | Enqueue throughput (burst, jobs/s) | Enqueue p95 (burst, ms) | End-to-end p50 / p95 at 100 jobs/s (s) | Processing capacity (jobs/min) | per worker |
|---|---|---|---|---|---|
| 1 | 488 | 156 | 0.06 / 0.10 | 8,830 | 8,830 |
| 2 | 450 | 542 | 0.06 / 0.10 | 18,074 | 9,037 |
| 4 | 489 | 168 | 0.06 / 0.09 | 34,500 | 8,625 |

**Burst: 500 jobs/s offered for 30 s (enqueue and processing share one Postgres)**

| Workers | Runs | enqueue/s (achieved) | enqueue p95 (ms) | dropped by k6 | jobs/min processed | e2e p50 (s) | e2e p95 (s) |
|---|---|---|---|---|---|---|---|
| 1 | 3 | 488 [465-494] | 156 [84-380] | 348 [189-1053] | 8281 [8248-8301] | 40.9 [39.2-41.1] | 73.4 [67.7-73.8] |
| 2 | 3 | 450 [424-500] | 542 [13-761] | 1480 [4-2291] | 14060 [13326-17634] | 17.6 [11.4-17.6] | 26.7 [20.2-27.1] |
| 4 | 3 | 489 [449-498] | 168 [35-1482] | 339 [48-1543] | 26686 [24575-29092] | 3.5 [3.2-4.9] | 4.8 [4.5-5.7] |

**Steady: 100 jobs/s for 45 s (default idle poll ceiling: 1 s)**

| Workers | Runs | enqueue/s | enqueue p95 (ms) | e2e p50 (s) | e2e p95 (s) |
|---|---|---|---|---|---|
| 1 | 3 | 100 [100-100] | 7 [4-12] | 0.06 [0.06-0.07] | 0.10 [0.10-0.30] |
| 2 | 3 | 100 [100-100] | 6 [5-8] | 0.06 [0.06-0.06] | 0.10 [0.10-0.10] |
| 4 | 3 | 100 [100-100] | 6 [5-47] | 0.06 [0.06-0.07] | 0.09 [0.09-0.18] |

**Steady, previous default idle poll ceiling of 5 s (same load)**

| Workers | Runs | enqueue/s | enqueue p95 (ms) | e2e p50 (s) | e2e p95 (s) |
|---|---|---|---|---|---|
| 1 | 3 | 100 [100-100] | 35 [11-178] | 0.08 [0.08-1.39] | 3.80 [3.24-4.73] |
| 2 | 3 | 100 [100-100] | 6 [5-22] | 0.06 [0.06-0.07] | 0.24 [0.12-0.74] |
| 4 | 3 | 100 [100-100] | 12 [8-20] | 0.07 [0.06-0.07] | 0.29 [0.10-2.62] |

**Steady, idle poll ceiling of 500 ms (same load)**

| Workers | Runs | enqueue/s | e2e p50 (s) | e2e p95 (s) |
|---|---|---|---|---|
| 1 | 3 | 100 [100-100] | 0.06 [0.06-0.06] | 0.11 [0.10-0.11] |

**Enqueue only: API with no workers, 1,500 jobs/s offered for 20 s**

| Workers | Runs | enqueue/s (achieved) | enqueue p50 (ms) | enqueue p95 (ms) | enqueue p99 (ms) | dropped by k6 |
|---|---|---|---|---|---|---|
| 0 | 3 | 1477 [1431-1481] | 0.7 [0.7-1.5] | 15 [14-261] | 234 [217-507] | 456 [373-1373] |

**Drain: 20,000 jobs pre-filled, no enqueue load (pure processing capacity)**

| Workers | Runs | window (s) | jobs/min | jobs/min per worker |
|---|---|---|---|---|
| 1 | 3 | 136 [134-139] | 8830 [8632-8969] | 8830 [8632-8969] |
| 2 | 3 | 66 [66-67] | 18074 [17810-18185] | 9037 [8905-9093] |
| 4 | 3 | 35 [34-36] | 34500 [33069-35384] | 8625 [8267-8846] |

**Kill: 100 emails/s + 12 slow (2 s) reports/s for 60 s; one of 4 workers SIGKILLed at t+25 s (10 s lease)**

| Workers | Runs | accepted (201) | succeeded | lost | jobs running when killed | attempts reclaimed (LEASE_EXPIRED) | e2e p95 (s) | e2e max (s) |
|---|---|---|---|---|---|---|---|---|
| 4 | 3 | 6722 [6722-6722] | 6722 [6722-6722] | 0 [0-0] | 8 [7-8] | 8 [8-8] | 7.9 [7.7-9.8] | 15.3 [15.3-19.1] |

Zero-lost-jobs verification: 42 processing runs (422,405 jobs: every accepted job reached a terminal state, none dead-lettered, one email per succeeded send-email job) and 3 enqueue-only runs (every 201 was stored). ALL PASSED.

## What the numbers say

- **Processing scales near-linearly with workers** when nothing else competes: about 8.6-9.0k jobs/min
  per worker by median (individual runs 8.3-9.1k; about 147 jobs/s, close to the 160/s ceiling of 8 slots at ~50 ms per job), giving 8.8k,
  18.1k and 34.5k jobs/min for 1, 2 and 4 workers (1.0x, 2.05x, 3.9x). Mid-run CPU samples (one `docker stats`
  snapshot from the first run of each, not averages) showed Postgres at about 16%, 23% and 50% of one core and each worker at
  roughly 15-25%, so the database was not the limit at these rates: the simulated handler latency was.
- **Under burst, enqueue and processing compete.** The API alone sustained about 1,480 enqueues/s
  (p95 15 ms), but while workers were draining it managed only about 450-490/s with p95 from 13 ms to
  1.5 s and k6 dropping up to ~2,300 scheduled requests in the worst run, because the API and workers
  share one Postgres and one machine. The 4-worker burst row is limited by the arrival rate (it drained
  almost as fast as jobs arrived), so its jobs/min understates 4-worker capacity: use the drain table.
  End-to-end latency in burst is dominated by the backlog (it is queue wait, not processing time).
- **At 100 jobs/s the queue adds about 0.1 s at p95** for every worker count (p50 about 60 ms, which is
  the handler's own ~50 ms). The tail is the idle-poll ceiling (below), not contention.
- **Crash during load loses nothing.** One of 4 workers was `SIGKILL`ed at t+25 s while holding 7-8
  running jobs; across 3 runs all accepted jobs (6,722 each) reached `SUCCEEDED`, 8 attempts per run were
  reclaimed via lease expiry and re-run elsewhere (max e2e 15-19 s, about the 10 s lease plus queueing),
  and the outbox held exactly one email per succeeded send-email job despite the re-runs.
- **Zero lost jobs:** in all 42 processing runs (422,405 jobs) the number of jobs the API accepted (HTTP 201)
  equalled the number in a terminal state, nothing was left `PENDING`/`RUNNING`, nothing was dead-lettered,
  and every succeeded `send-email` job had exactly one email (`collect.py` checks all of this and fails the
  run otherwise). The 3 enqueue-only runs verified that every 201 was stored.

## Problems the load test found (and fixed)

1. **Saturated workers refilled slots only once per poll interval.** My first calibration run
   (1 worker, 500 jobs/s) processed about **2,287 jobs/min**; after waking the poller
   as soon as a slot frees it processed about **7,726 jobs/min** (3.4x; single runs
   each, in `loadtest/results/calibration/`). The old loop capped throughput at roughly
   `concurrency / poll-interval` however fast the handlers were. `SlotRefillTest` reproduces it: with a
   500 ms poll interval the old loop took 6.9 s to drain 100 jobs and fails the test's 3 s bound; the fixed
   loop passes it.
2. **The idle-poll backoff ceiling was a latency floor.** With the original 5 s ceiling, one worker at
   100 jobs/s had an end-to-end **p95 of 3.8 s** (median 0.08 s): jobs arriving after a quiet period waited
   for the next poll. Capping the backoff at 500 ms gave p95 0.11 s (same load, 3 runs each), which
   confirmed the cause, so the default is now 1 s (p95 about 0.1 s above). The cost is one trivial query
   per second per idle worker. The structural fix is `LISTEN/NOTIFY` (see the README's design trade-offs).
3. **My first kill scenario often killed a worker that held nothing** (median 0 reclaimed attempts over 3
   runs), so it proved little. It now keeps worker slots busy with a trickle of slow jobs so the victim
   always holds work (the superseded runs are in `loadtest/results/superseded/`).
