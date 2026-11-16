# ADR-001: Distributed Idempotency via Redis Atomic Locks & Response Caching

## Status
Accepted

## Context
Mobile edge devices operating in fulfillment centers experience frequent Wi-Fi handoff drops. A client frequently transmits an inventory mutation (e.g., `DEDUCT_SKU`), times out waiting for the HTTP response, and automatically retries the exact same payload across a load-balanced API cluster. Without distributed deduplication, concurrent retries hitting different API pods result in double deductions and inventory drift.

## Decision
We implement an edge-to-cloud idempotency layer using **client-generated UUIDv7 `X-Idempotency-Key` headers** backed by **Redis `SET NX EX` distributed locks** and a **24-hour response replay cache**.

## Alternatives Considered
1. **Database Unique Constraint Only:** Rejected for primary request gating because concurrent in-flight retries still hit database connection pools and trigger rollback contention under thundering-herd reconnection spikes. (Kept as a secondary defense-in-depth constraint in PostgreSQL).
2. **In-Memory Node Caching:** Rejected because requests are load-balanced across multiple Kubernetes pods; a retry frequently routes to a different pod than the original request.

## Consequences & Trade-Offs
* **Positive:** Sub-2ms deduplication latency at the gateway edge; protects PostgreSQL write capacity during network recovery spikes; returns deterministic responses (`X-Idempotent-Replay: true`) to retrying clients.
* **Negative / Mitigation:** Introduces Redis as a critical-path dependency. Mitigated via Redis AOF persistence, Sentinel/Cluster failover, and failing closed (`503 Service Unavailable`) on cache unreachable states to preserve strict inventory consistency (CP over AP).
