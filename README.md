# Distributed Idempotent Sync Gateway

A reference cloud-native gateway engineered to ingest high-concurrency offline mutations from distributed mobile/web edge clients with **at-most-once execution guarantees**.

## Core Distributed Systems Patterns
* **Distributed Idempotency (`Redis SET NX`):** Prevents duplicate state mutations across load-balanced API nodes during network retries.
* **Optimistic Concurrency Control:** Version-vector checks prevent lost updates when multiple operators modify the same entity concurrently.
* **Thundering-Herd Protection:** Edge rate limiting and deterministic retry headers stabilize backend databases when hundreds of offline devices reconnect simultaneously.

## Quick Start
```bash
docker-compose up --build
```
# Run thundering-herd simulation against local Docker cluster
k6 run load-tests/thundering-herd.js


## Architecture Documentation
* [ADR-001: Distributed Idempotency Strategy](./docs/ADR-001-idempotency.md)
