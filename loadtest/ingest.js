// Ingest throughput: POST /v1/events at a constant arrival rate.
//
//   k6 run -e OPERATOR_TOKEN=... -e RATE=300 -e DURATION=60s loadtest/ingest.js
//
// ENDPOINTS=0 measures the API + Postgres write path alone (the outbox row is still written
// and drained by the relay). IDEMPOTENT_RATIO (default 0.1) of requests carry an
// Idempotency-Key, half of which are deliberate replays, so the row-lock claim path is part
// of the mix. Thresholds fail the run: < 1% errors, p95 < P95_MS (250), p99 < P99_MS (750).
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { BASE_URL, auth, eventBody, provision } from './lib.js';

const RATE = parseInt(__ENV.RATE || '200', 10);
const IDEMPOTENT_RATIO = parseFloat(__ENV.IDEMPOTENT_RATIO || '0.1');
const accepted = new Counter('events_accepted');
const replayed = new Counter('events_replayed');

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: parseInt(__ENV.VUS || '50', 10),
      maxVUs: parseInt(__ENV.MAX_VUS || '400', 10),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:ingest}': [`p(95)<${__ENV.P95_MS || 250}`, `p(99)<${__ENV.P99_MS || 750}`],
    checks: ['rate>0.99'],
    dropped_iterations: [`count<${__ENV.MAX_DROPPED || RATE}`],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  return provision(parseInt(__ENV.ENDPOINTS || '0', 10));
}

export default function (ctx) {
  const extra = {};
  let seq = `${__VU}-${__ITER}`;
  if (Math.random() < IDEMPOTENT_RATIO) {
    // Half fresh keys, half replays of a small per-VU key space (same key => same body).
    if (Math.random() < 0.5) {
      seq = `r-${__VU}-${__ITER % 5}`;
    }
    extra['Idempotency-Key'] = `k6-${ctx.slug}-${seq}`;
  }
  const res = http.post(`${BASE_URL}/v1/events`, eventBody(seq), {
    headers: auth(ctx.key, extra),
    tags: { name: 'ingest' },
  });
  check(res, { 'ingest 202': (r) => r.status === 202 });
  if (res.status === 202) {
    if (res.headers['Idempotent-Replayed'] === 'true') replayed.add(1);
    else accepted.add(1);
  }
}
