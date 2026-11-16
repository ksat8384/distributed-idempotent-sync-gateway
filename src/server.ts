import express, { Request, Response } from "express";
import { Pool } from "pg";
import { distributedRateLimiter } from "./middleware/rateLimiter";
import { idempotencyMiddleware } from "./middleware/idempotency";
import {
  InventoryEntity,
  OfflineMutationEvent,
  resolveInventoryMutation,
} from "./sync/conflictResolver";
import { OutboxWorker, EventPublisher } from "./sync/outboxWorker";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const NODE_ID = process.env.NODE_ID || "api-node-local";

const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20, // Bounded connection pool per API pod
  idleTimeoutMillis: 30000,
});

// Stub EventPublisher (swap with KafkaJS / AWS EventBridge / Redis Streams in prod)
const brokerPublisher: EventPublisher = {
  async publish(topic, key, message) {
    console.log(
      `[Broker:${topic}] key=${key} payload=${JSON.stringify(message)}`
    );
  },
};

const outboxWorker = new OutboxWorker(db, brokerPublisher);

// Liveness & Readiness probe for Kubernetes / Nginx load balancer
app.get("/health", async (_req: Request, res: Response) => {
  try {
    await db.query("SELECT 1");
    res.status(200).json({ status: "healthy", nodeId: NODE_ID });
  } catch {
    res.status(503).json({ status: "degraded", nodeId: NODE_ID });
  }
});

/**
 * Edge-to-Cloud Offline Mutation Sync Endpoint
 * Pipeline order matters:
 * 1. Rate Limiter (protects Redis & DB from thundering-herd floods)
 * 2. Idempotency Guard (deduplicates network retries across pods)
 * 3. Transactional State Mutation + Outbox Append (atomic ACID commit)
 */
app.post(
  "/api/v1/inventory/sync",
  distributedRateLimiter({ capacity: 25, refillRate: 5 }),
  idempotencyMiddleware,
  async (req: Request, res: Response): Promise<void> => {
    const event = req.body as OfflineMutationEvent;

    if (
      !event?.sku ||
      !event?.deviceId ||
      typeof event.baseVersion !== "number"
    ) {
      res
        .status(400)
        .json({
          error:
            "Invalid mutation payload: sku, deviceId, and baseVersion required",
        });
      return;
    }

    const client = await db.connect();
    try {
      await client.query("BEGIN");

      // Row-level pessimistic lock prevents concurrent pod races on the same SKU
      const { rows } = await client.query<InventoryEntity>(
        `
        SELECT sku, quantity, location_zone AS "locationZone", version,
               last_updated_by AS "lastUpdatedBy", updated_at AS "updatedAt"
        FROM inventory
        WHERE sku = $1
        FOR UPDATE
        `,
        [event.sku]
      );

      if (rows.length === 0) {
        await client.query("ROLLBACK");
        res.status(404).json({ error: `SKU ${event.sku} not found` });
        return;
      }

      const currentServerState = rows[0];
      const resolution = resolveInventoryMutation(currentServerState, event);

      if (resolution.status === "CONFLICT_QUEUED") {
        // Persist unresolved conflict for supervisor review without losing audit trail
        await client.query(
          `
          INSERT INTO conflict_exceptions (event_id, sku, device_id, reason, client_payload, server_snapshot)
          VALUES ($1, $2, $3, $4, $5, $6)
          `,
          [
            event.eventId,
            event.sku,
            event.deviceId,
            resolution.reason,
            JSON.stringify(event),
            JSON.stringify(resolution.serverState),
          ]
        );

        await client.query("COMMIT");
        res.status(409).json({
          status: "CONFLICT_QUEUED",
          reason: resolution.reason,
          serverState: resolution.serverState,
        });
        return;
      }

      const { updatedEntity, mergeNote } = resolution;

      // 1. Update materialized inventory state
      await client.query(
        `
        UPDATE inventory
        SET quantity = $1, location_zone = $2, version = $3,
            last_updated_by = $4, updated_at = $5
        WHERE sku = $6
        `,
        [
          updatedEntity.quantity,
          updatedEntity.locationZone,
          updatedEntity.version,
          updatedEntity.lastUpdatedBy,
          updatedEntity.updatedAt,
          updatedEntity.sku,
        ]
      );

      // 2. Append domain event to Transactional Outbox in the SAME ACID transaction
      await client.query(
        `
        INSERT INTO outbox_events (id, aggregate_id, event_type, payload, status)
        VALUES ($1, $2, 'INVENTORY_MUTATED', $3, 'PENDING')
        `,
        [event.eventId, updatedEntity.sku, JSON.stringify(updatedEntity)]
      );

      await client.query("COMMIT");

      res.setHeader("X-Served-By", NODE_ID);
      res.status(200).json({
        status: "APPLIED",
        entity: updatedEntity,
        ...(mergeNote && { mergeNote }),
      });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(`[${NODE_ID}] Sync transaction failed:`, err);
      res.status(500).json({ error: "Internal sync transaction failure" });
    } finally {
      client.release();
    }
  }
);

const server = app.listen(PORT, () => {
  console.log(`[${NODE_ID}] Sync gateway listening on port ${PORT}`);
  outboxWorker.start();
});

// Graceful shutdown for Kubernetes pod termination / rolling deployments
function shutdown(signal: string): void {
  console.log(`[${NODE_ID}] Received ${signal}. Draining connections...`);
  outboxWorker.stop();
  server.close(async () => {
    await db.end();
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
