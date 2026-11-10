// End-to-end: publish at a constant rate, then wait until the mock receiver has acknowledged
// every delivery, and fail the run on any lost or duplicated-and-acknowledged message.
//
//   k6 run -e OPERATOR_TOKEN=... -e RATE=200 -e DURATION=60s -e ENDPOINTS=4 \
//          -e RECEIVER_QUERY='latency_ms=20&jitter_ms=30&failure_rate=0.02' loadtest/e2e.js
//
// After the ingest phase, teardown counts the tenant's accepted events through the API
// (expected deliveries = events x ENDPOINTS) and polls the receiver's /stats until it has
// acknowledged that many distinct (endpoint, webhook-id) pairs or DRAIN_TIMEOUT_S passes.
// The receiver must be dedicated to the run (it is reset in setup).
//
// Custom metrics (thresholds in brackets):
//   deliveries_expected / deliveries_acked        counters
//   deliveries_lost                    [count==0] expected - acknowledged after the drain
//   deliveries_acked_duplicates        [count<=MAX_DUPLICATES (0)] acked more than once
//   e2e_lag_p50_ms / e2e_lag_p95_ms    [p95 < E2E_P95_MS (5000)] occurred_at -> receiver
//   delivery_throughput_per_s          acknowledged deliveries / receiver window
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Gauge } from 'k6/metrics';
import { BASE_URL, ENDPOINTS, auth, countEvents, eventBody, provision, receiverStats, resetReceiver } from './lib.js';

const RATE = parseInt(__ENV.RATE || '100', 10);
const DRAIN_TIMEOUT_S = parseInt(__ENV.DRAIN_TIMEOUT_S || '120', 10);

const expected = new Counter('deliveries_expected');
const acked = new Counter('deliveries_acked');
const lost = new Counter('deliveries_lost');
const ackedDuplicates = new Counter('deliveries_acked_duplicates');
const lagP50 = new Gauge('e2e_lag_p50_ms');
const lagP95 = new Gauge('e2e_lag_p95_ms');
const lagP99 = new Gauge('e2e_lag_p99_ms');
const throughput = new Gauge('delivery_throughput_per_s');
const drainSeconds = new Gauge('drain_seconds');

export const options = {
  scenarios: {
    publish: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: parseInt(__ENV.VUS || '30', 10),
      maxVUs: parseInt(__ENV.MAX_VUS || '300', 10),
    },
  },
  setupTimeout: '60s',
  teardownTimeout: `${DRAIN_TIMEOUT_S + 120}s`,
  thresholds: {
    'http_req_failed{name:ingest}': ['rate<0.01'],
    'http_req_duration{name:ingest}': [`p(95)<${__ENV.P95_MS || 500}`],
    checks: ['rate>0.99'],
    deliveries_lost: ['count==0'],
    deliveries_acked_duplicates: [`count<=${__ENV.MAX_DUPLICATES || 0}`],
    deliveries_acked: ['count>0'],
    e2e_lag_p95_ms: [`value<${__ENV.E2E_P95_MS || 5000}`],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  resetReceiver();
  return provision(ENDPOINTS);
}

export default function (ctx) {
  const res = http.post(`${BASE_URL}/v1/events`, eventBody(`${__VU}-${__ITER}`), {
    headers: auth(ctx.key),
    tags: { name: 'ingest' },
  });
  check(res, { 'ingest 202': (r) => r.status === 202 });
}

export function teardown(ctx) {
  const want = countEvents(ctx.key) * ENDPOINTS;
  const started = Date.now();
  let stats = receiverStats();
  while (stats.acked_unique < want && Date.now() - started < DRAIN_TIMEOUT_S * 1000) {
    sleep(0.5);
    stats = receiverStats();
  }
  // Let late duplicates (if any) arrive before judging.
  sleep(parseFloat(__ENV.SETTLE_S || '2'));
  stats = receiverStats();
  expected.add(want);
  acked.add(stats.acked_unique);
  lost.add(Math.max(0, want - stats.acked_unique));
  ackedDuplicates.add(stats.acked_duplicates);
  if (stats.lag_ms.p50 !== null) {
    lagP50.add(stats.lag_ms.p50);
    lagP95.add(stats.lag_ms.p95);
    lagP99.add(stats.lag_ms.p99);
  }
  if (stats.window_s) throughput.add(stats.acked / stats.window_s);
  drainSeconds.add((Date.now() - started) / 1000);
  console.log(`e2e: expected=${want} receiver=${JSON.stringify(stats)}`);
}
