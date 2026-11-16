import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

// Custom metrics to prove idempotency & resilience under load
const idempotentReplays = new Counter("idempotent_replays_caught");
const concurrentConflicts = new Counter("concurrent_locks_blocked");

export const options = {
  scenarios: {
    // Simulates a sudden Wi-Fi recovery spike: 0 -> 500 concurrent edge devices in 5 seconds
    thundering_herd_reconnect: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "5s", target: 500 }, // Sudden network recovery burst
        { duration: "20s", target: 500 }, // Sustained batch drain
        { duration: "5s", target: 0 }, // Queue drained
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<150"], // 95% of sync requests must complete under 150ms
    http_req_failed: ["rate<0.01"], // Less than 1% unexpected 5xx failures
  },
};

const BASE_URL = __ENV.TARGET_URL || "http://localhost:8080";

export default function () {
  // Simulate 20% duplicate retry rate (flaky edge devices retrying timed-out requests)
  const isDuplicateRetry = Math.random() < 0.2;
  const sharedKeyPool = `key-retry-bucket-${Math.floor(Math.random() * 50)}`;
  const idempotencyKey = isDuplicateRetry
    ? sharedKeyPool
    : `key-${__VU}-${__ITER}-${Date.now()}`;

  const payload = JSON.stringify({
    deviceId: `scanner-${__VU}`,
    sku: `SKU-${Math.floor(Math.random() * 100)}`,
    delta: -1,
    timestamp: new Date().toISOString(),
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
      "X-Idempotency-Key": idempotencyKey,
    },
  };

  const res = http.post(`${BASE_URL}/api/v1/inventory/sync`, payload, params);

  // Track distributed deduplication behavior
  if (res.headers["X-Idempotent-Replay"] === "true") {
    idempotentReplays.add(1);
  }
  if (res.status === 409) {
    concurrentConflicts.add(1);
  }

  check(res, {
    "valid distributed outcome (200/201/409/429)": (r) =>
      [200, 201, 409, 429].includes(r.status),
    "no 5xx server collapse under spike": (r) => r.status < 500,
  });

  // Jittered sleep simulating exponential backoff + jitter on mobile clients
  sleep(Math.random() * 0.5);
}
