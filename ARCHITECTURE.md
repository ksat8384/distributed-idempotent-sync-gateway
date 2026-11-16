# System Architecture: Distributed Idempotent Sync Gateway

## 1. Architectural Overview & System Goals
The **Distributed Idempotent Sync Gateway** is a horizontally scalable edge-to-cloud ingestion platform designed to process high-concurrency offline mutations from mobile and web clients operating in degraded network environments.

### Core Non-Functional Requirements (NFRs)
* **At-Most-Once State Mutation:** Duplicate retries triggered by mobile network timeouts must never cause double-deductions or state corruption.
* **At-Least-Once Event Delivery:** Every committed state transition must reliably propagate to downstream microservices (ERP, Analytics, Notifications) without dual-write data loss.
* **Thundering-Herd Resilience:** Must absorb sudden reconnection bursts (e.g., 500+ edge scanners reconnecting simultaneously after a Wi-Fi outage) without exhausting database connection pools.
* **Deterministic Conflict Convergence:** Concurrent offline edits to the same entity must resolve predictably without silent overwrites.

---

## 2. End-to-End Request & Event Topology

```text
   [ Edge Mobile / Web Clients (Offline Outbox Queues) ]

             |              |              |
             +--------------+--------------+
                            | (HTTPS + X-Idempotency-Key + X-Device-Id)
                            v
               +-------------------------+

               |   Nginx / K8s Ingress   |
               +------------+------------+
                            | (Round-Robin Load Balancing)
             +--------------+--------------+
             v                             v
+-------------------------+   +-------------------------+

|    Sync API Pod #1      |   |    Sync API Pod #2      |
|  1. Token-Bucket Limit  |   |  1. Token-Bucket Limit  |
|  2. Idempotency Guard   |   |  2. Idempotency Guard   |
|  3. Conflict Resolver   |   |  3. Conflict Resolver   |
+------+-----------+------+   +------+-----------+------+

       |           |                 |           |
       | (Lua/NX)  +--------+--------+           | (ACID Tx)
       v                    |                    v
+--------------+            |         +---------------------+

| Redis Cluster|            |         |   PostgreSQL (HA)   |
| - Rate Buckets|           |         | - inventory (State) |
| - Idem Locks |            |         | - outbox_events     |
| - Resp Cache |            |         | - conflict_exceptions|
+--------------+            |         +----------+----------+

                            |                    |
                            |                    | (FOR UPDATE SKIP LOCKED)
                            |                    v
                            |         +---------------------+

                            |         | Outbox Relay Worker |
                            |         | - Exponential Jitter|
                            |         | - DLQ Isolation     |
                            |         +----------+----------+

                            |                    |
                            |                    v
                            |         +---------------------+
                            +-------->| Event Broker (Kafka)|
                                      +---------------------+
```

---

## 3. Request Pipeline & Defense-in-Depth Ordering
Every incoming `POST /api/v1/inventory/sync` request passes through three ordered layers designed to shed load early before touching primary storage:

1. **Layer 1 — Distributed Token-Bucket Rate Limiter (`src/middleware/rateLimiter.ts`):**
   * Executes an atomic **Redis Lua script** per `X-Device-Id`.
   * Permits controlled bursts (`capacity: 25`) so a reconnecting device can flush its local outbox, then enforces a steady drain (`refillRate: 5 req/s`).
   * Returns `429 Too Many Requests` with a randomized `Retry-After` jitter header to desynchronize client retry storms.
2. **Layer 2 — Distributed Idempotency Guard (`src/middleware/idempotency.ts`):**
   * Checks Redis (`idem:resp:<key>`) for previously completed mutations (`24h TTL`). If found, returns the cached response immediately (`X-Idempotent-Replay: true`).
   * Acquires an atomic `SET NX EX` lock (`idem:lock:<key>`) to block concurrent in-flight retries across different API pods (`409 Conflict`).
3. **Layer 3 — ACID Mutation & Transactional Outbox (`src/server.ts`):**
   * Locks the target SKU row (`SELECT ... FOR UPDATE`), evaluates state convergence via `resolveInventoryMutation()`, updates `inventory`, and appends a domain event to `outbox_events` inside a **single PostgreSQL transaction**.

---

## 4. Data Consistency & Conflict Resolution Model
Instead of relying on naive Last-Write-Wins (LWW), the conflict resolver (`src/sync/conflictResolver.ts`) classifies fields by mathematical commutativity:

* **Commutative Operations (`quantityDelta`):** Inventory adjustments (`-2`, `+5`) are applied as relative deltas against current server state, allowing stale-version offline events to converge safely without lost updates—provided domain invariants (`quantity >= 0`) hold.
* **Non-Commutative Metadata (`locationZone`):** Protected by **Optimistic Concurrency Control (OCC)**. If `client.baseVersion < server.version`, the mutation is rejected and persisted to `conflict_exceptions` for supervisor review.

---

## 5. Failure Mode & Degraded-State Matrix

| Dependency Failure | System Behavior | CAP / Architectural Rationale |
| :--- | :--- | :--- |
| **Redis Unreachable (Rate Limiter)** | **Fail-Open** (`next()`) | Preserves warehouse floor availability; downstream DB connection pools still bound concurrency. |
| **Redis Unreachable (Idempotency)** | **Fail-Closed** (`503 Service Unavailable`) | **CP over AP:** Prevents duplicate inventory deductions when deduplication guarantees cannot be upheld. |
| **Event Broker Outage (Kafka)** | **Buffer in PostgreSQL Outbox** | API ingestion continues unaffected; `OutboxWorker` retries with capped exponential backoff + jitter once broker recovers. |
| **Poison-Pill Event Payload** | **Dead-Letter Isolation (`DEAD_LETTER`)** | After `MAX_RETRIES (5)`, failing events are quarantined so healthy SKU events continue streaming unblocked. |
| **Kubernetes Pod Termination** | **Graceful Drain (`SIGTERM`)** | Stops outbox polling loop, closes HTTP listener, and drains active PostgreSQL pool connections before exit. |

---

## 6. Load Verification (`load-tests/thundering-herd.js`)
System resilience is continuously validated using **k6** by simulating a `0 -> 500` concurrent device reconnection surge within 5 seconds with a **20% duplicate retry collision rate**, verifying `p95 < 150ms` latency and zero `5xx` server failures.
