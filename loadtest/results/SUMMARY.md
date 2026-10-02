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

Zero-lost-jobs verification: 45 runs, ALL PASSED.
