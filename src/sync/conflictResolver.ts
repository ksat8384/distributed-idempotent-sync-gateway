export interface InventoryEntity {
  sku: string;
  quantity: number;
  locationZone: string;
  version: number; // Monotonic server version for Optimistic Concurrency Control
  lastUpdatedBy: string;
  updatedAt: string; // ISO-8601 server timestamp
}

export interface OfflineMutationEvent {
  eventId: string;
  deviceId: string;
  sku: string;
  baseVersion: number; // The entity version cached on the device when offline scan occurred
  quantityDelta?: number; // Commutative delta (e.g., -2 picked, +10 restocked)
  locationZone?: string; // Non-commutative metadata field
  clientTimestamp: string;
}

export type ResolutionOutcome =
  | { status: "APPLIED"; updatedEntity: InventoryEntity; mergeNote?: string }
  | { status: "CONFLICT_QUEUED"; reason: string; serverState: InventoryEntity };

/**
 * Deterministic Conflict Resolver for Offline-First Edge Sync
 *
 * Strategy:
 * 1. Commutative Operations (Inventory Deltas): Safe to merge even when client baseVersion
 *    is stale, provided resulting stock does not violate domain invariants (e.g., < 0).
 * 2. Non-Commutative Metadata (Location/Zone): Enforces Optimistic Concurrency Control (OCC).
 *    Stale writes are rejected and routed to a supervisor exception queue rather than silently overwritten.
 */
export function resolveInventoryMutation(
  serverState: InventoryEntity,
  event: OfflineMutationEvent
): ResolutionOutcome {
  const isStaleVersion = event.baseVersion < serverState.version;
  const hasMetadataChange =
    event.locationZone !== undefined &&
    event.locationZone !== serverState.locationZone;

  // 1. Non-commutative field conflict on a stale version -> Route to Exception Queue
  if (isStaleVersion && hasMetadataChange) {
    return {
      status: "CONFLICT_QUEUED",
      reason: `Stale baseVersion (${event.baseVersion} < ${serverState.version}) on non-commutative field 'locationZone'`,
      serverState,
    };
  }

  // 2. Commutative Delta Merge (CRDT-style counter convergence)
  let nextQuantity = serverState.quantity;
  if (event.quantityDelta !== undefined) {
    nextQuantity = serverState.quantity + event.quantityDelta;

    // Domain Invariant Check: Prevent negative physical inventory
    if (nextQuantity < 0) {
      return {
        status: "CONFLICT_QUEUED",
        reason: `Inventory invariant violation: applying delta (${event.quantityDelta}) to current stock (${serverState.quantity}) yields negative count`,
        serverState,
      };
    }
  }

  // 3. Construct converged state and increment monotonic version
  const updatedEntity: InventoryEntity = {
    ...serverState,
    quantity: nextQuantity,
    locationZone: event.locationZone ?? serverState.locationZone,
    version: serverState.version + 1,
    lastUpdatedBy: event.deviceId,
    updatedAt: new Date().toISOString(),
  };

  return {
    status: "APPLIED",
    updatedEntity,
    ...(isStaleVersion && {
      mergeNote: `Converged commutative delta across version gap (${event.baseVersion} -> ${updatedEntity.version})`,
    }),
  };
}
