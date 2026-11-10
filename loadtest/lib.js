// Shared configuration and provisioning for the hookrelay k6 scripts.
//
// Every run provisions its own tenant through the operator API, so runs never interfere and
// need nothing but OPERATOR_TOKEN. Endpoints point at the mock receiver
// (`hookrelay-mock-receiver`); RECEIVER_QUERY is appended to each endpoint URL and overrides
// the receiver's behaviour for that endpoint (latency_ms, jitter_ms, failure_rate,
// failure_status, retry_after_s, hang_rate, hang_ms).
import http from 'k6/http';
import { check, fail } from 'k6';

export const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8000';
// The receiver as the *worker* reaches it (inside compose: http://mock-receiver:9000).
export const RECEIVER_URL = __ENV.RECEIVER_URL || 'http://127.0.0.1:9000';
// The receiver as *k6* reaches it (stats / reset); defaults to RECEIVER_URL.
export const RECEIVER_ADMIN_URL = __ENV.RECEIVER_ADMIN_URL || RECEIVER_URL;
export const OPERATOR_TOKEN = __ENV.OPERATOR_TOKEN || '';
export const EVENT_TYPE = __ENV.EVENT_TYPE || 'load.test';
export const PAYLOAD_BYTES = parseInt(__ENV.PAYLOAD_BYTES || '512', 10);
export const ENDPOINTS = parseInt(__ENV.ENDPOINTS || '1', 10);
export const RECEIVER_QUERY = __ENV.RECEIVER_QUERY || '';
export const MAX_IN_FLIGHT = parseInt(__ENV.MAX_IN_FLIGHT || '100', 10);

const JSON_HEADERS = { 'Content-Type': 'application/json' };

export function auth(token, extra) {
  return Object.assign({ Authorization: `Bearer ${token}` }, JSON_HEADERS, extra || {});
}

function must(res, status, what) {
  if (!check(res, { [`${what} ${status}`]: (r) => r.status === status })) {
    fail(`${what}: expected ${status}, got ${res.status}: ${res.body}`);
  }
  return res.json();
}

// Tenant + admin key + event type + ENDPOINTS endpoints (subscribed to `load.*`).
export function provision(endpoints = ENDPOINTS) {
  if (!OPERATOR_TOKEN) fail('OPERATOR_TOKEN is required to provision a tenant');
  const op = auth(OPERATOR_TOKEN);
  const slug = `k6-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const tenant = must(
    http.post(`${BASE_URL}/admin/tenants`, JSON.stringify({ slug, name: slug }), { headers: op }),
    201,
    'create tenant',
  );
  const key = must(
    http.post(
      `${BASE_URL}/admin/tenants/${tenant.id}/api-keys`,
      JSON.stringify({ name: 'k6', role: 'admin' }),
      { headers: op },
    ),
    201,
    'issue api key',
  ).key;
  const h = auth(key);
  must(http.post(`${BASE_URL}/v1/event-types`, JSON.stringify({ name: EVENT_TYPE }), { headers: h }), 201, 'event type');
  const ids = [];
  for (let i = 0; i < endpoints; i++) {
    const query = RECEIVER_QUERY ? `?${RECEIVER_QUERY}` : '';
    const ep = must(
      http.post(
        `${BASE_URL}/v1/endpoints`,
        JSON.stringify({ url: `${RECEIVER_URL}/hook/${slug}/${i}${query}`, max_in_flight: MAX_IN_FLIGHT }),
        { headers: h },
      ),
      201,
      'create endpoint',
    );
    must(
      http.post(`${BASE_URL}/v1/endpoints/${ep.id}/subscriptions`, JSON.stringify({ event_type: 'load.*' }), { headers: h }),
      201,
      'subscribe',
    );
    ids.push(ep.id);
  }
  return { tenantId: tenant.id, slug, key, endpointIds: ids };
}

const FILLER = 'x'.repeat(Math.max(0, PAYLOAD_BYTES - 64));

// The payload depends only on `seq`, so an Idempotency-Key replay can resend the exact body
// (a different body under the same key is a 422 by design).
export function eventBody(seq) {
  return JSON.stringify({ event_type: EVENT_TYPE, payload: { seq: String(seq), filler: FILLER } });
}

// Accepted events for this tenant (keyset pagination over GET /v1/events, 100 per page).
export function countEvents(key) {
  let total = 0;
  let cursor = '';
  for (let page = 0; page < 100000; page++) {
    const url = `${BASE_URL}/v1/events?limit=100${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
    const body = must(http.get(url, { headers: auth(key) }), 200, 'list events');
    total += body.data.length;
    if (!body.has_more) break;
    cursor = body.next_cursor;
  }
  return total;
}

export function receiverStats() {
  return must(http.get(`${RECEIVER_ADMIN_URL}/stats`), 200, 'receiver stats');
}

export function resetReceiver() {
  must(http.post(`${RECEIVER_ADMIN_URL}/reset`), 200, 'receiver reset');
}
