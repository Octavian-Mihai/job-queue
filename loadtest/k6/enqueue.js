// Enqueues jobs at a constant arrival rate (open model: new requests arrive on schedule whether or
// not earlier ones have finished, like real clients). Everything else (drain time, end-to-end
// latency, lost-job check) is measured by loadtest/collect.py from the database afterwards.
//
//   RATE=500 DURATION=30s NAME=burst-w1 k6 run enqueue.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '300');
const DURATION = __ENV.DURATION || '60s';
const NAME = __ENV.NAME || 'run';
const REPORT_RATE = parseInt(__ENV.REPORT_RATE || '0'); // slow generate-report jobs per second
const REPORT_SLEEP_MS = parseInt(__ENV.REPORT_SLEEP_MS || '2000');

const accepted = new Counter('jobs_accepted'); // HTTP 201: the API promised to run this job
const rejected = new Counter('jobs_rejected'); // anything else

const scenarios = {
  enqueue: {
    executor: 'constant-arrival-rate',
    exec: 'enqueueEmail',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: 100,
    maxVUs: 1000,
  },
};
if (REPORT_RATE > 0) {
  // A trickle of slow jobs keeps worker slots occupied, so a worker killed mid-test is
  // guaranteed to be holding in-flight jobs when it dies.
  scenarios.reports = {
    executor: 'constant-arrival-rate',
    exec: 'enqueueReport',
    rate: REPORT_RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: 50,
    maxVUs: 200,
  };
}

export const options = {
  scenarios,
  thresholds: {
    'http_req_failed': ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function post(payload) {
  const res = http.post(`${BASE}/jobs`, JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'enqueue' },
  });
  const ok = check(res, { 'created (201)': (r) => r.status === 201 });
  if (ok) {
    accepted.add(1);
  } else {
    rejected.add(1);
  }
}

// send-email with simulate=ok: ~20-80 ms of simulated SMTP latency, then an idempotent insert.
export function enqueueEmail() {
  post({
    type: 'send-email',
    payload: { to: `user${__VU}-${__ITER}@example.com`, subject: 'load test', simulate: 'ok' },
  });
}

export function enqueueReport() {
  post({ type: 'generate-report', payload: { sleepMs: REPORT_SLEEP_MS } });
}

export function handleSummary(data) {
  return { [`/loadtest/results/${NAME}-k6.json`]: JSON.stringify(data, null, 2) };
}
