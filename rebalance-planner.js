/**
 * Schedules moves only after sufficient healthy replicas already exist.
 * Draining nodes outrank capacity balancing, which in turn uses a high-water
 * threshold so normal temporary load changes do not cause replica churn.
 */
export class RebalancePlanner {
  #metadata;
  #placement;
  #queue;
  #highWatermarkPercent;

  constructor({ metadata, placement, queue, highWatermarkPercent = 85 }) {
    if (!metadata || typeof metadata.listManifests !== "function" || typeof metadata.getReplicaSet !== "function" || typeof metadata.listNodes !== "function") {
      throw new TypeError("metadata must support manifests, replica state, and nodes");
    }
    if (!placement || typeof placement.assessReplicaSet !== "function") throw new TypeError("placement must support assessReplicaSet()");
    if (!queue || typeof queue.enqueue !== "function") throw new TypeError("queue must support enqueue()");
    if (!Number.isInteger(highWatermarkPercent) || highWatermarkPercent < 1 || highWatermarkPercent > 100) {
      throw new TypeError("highWatermarkPercent must be an integer from 1 to 100");
    }
    this.#metadata = metadata;
    this.#placement = placement;
    this.#queue = queue;
    this.#highWatermarkPercent = highWatermarkPercent;
  }

  async plan({ maxJobs = 1_000 } = {}) {
    if (!Number.isSafeInteger(maxJobs) || maxJobs < 0) throw new TypeError("maxJobs must be a non-negative safe integer");
    const nodes = this.#metadata.listNodes({ includeRemoved: true });
    const nodeById = new Map(nodes.map((node) => [node.nodeId, node]));
    const summary = { scannedChunks: 0, enqueuedJobs: 0, skippedAtRiskChunks: 0, reasons: { draining: 0, overCapacity: 0 } };

    for (const manifest of this.#metadata.listManifests()) {
      if (summary.enqueuedJobs >= maxJobs) break;
      for (const chunk of manifest.chunks) {
        if (summary.enqueuedJobs >= maxJobs) break;
        summary.scannedChunks += 1;
        const replicaSet = this.#metadata.getReplicaSet(manifest.objectId, { version: manifest.version, chunkId: chunk.chunkId });
        const healthyReplicas = replicaSet.replicas.filter((replica) => replica.state === "healthy");
        const assessment = this.#placement.assessReplicaSet({
          policyId: manifest.metadata.policyId ?? "standard",
          replicaNodeIds: healthyReplicas.map((replica) => replica.nodeId),
          includeDraining: true
        });
        // Never remove or move away from a replica while the current set is
        // below the desired durability target; the repair worker owns that.
        if (assessment.replicaDeficit > 0) {
          summary.skippedAtRiskChunks += 1;
          continue;
        }
        for (const replica of healthyReplicas) {
          if (summary.enqueuedJobs >= maxJobs) break;
          const node = nodeById.get(replica.nodeId);
          const reason = rebalanceReason(node, this.#highWatermarkPercent);
          if (!reason) continue;
          this.#queue.enqueue({
            objectId: manifest.objectId,
            version: manifest.version,
            chunk,
            sourceNodeId: replica.nodeId,
            reasons: [reason],
            priority: reason === "DRAINING" ? 95 : 50,
            observedAt: new Date().toISOString()
          });
          summary.enqueuedJobs += 1;
          if (reason === "DRAINING") summary.reasons.draining += 1;
          else summary.reasons.overCapacity += 1;
        }
      }
    }
    return Object.freeze(summary);
  }
}

function rebalanceReason(node, highWatermarkPercent) {
  if (!node || node.state === "removed" || node.state === "draining") return "DRAINING";
  const utilizationPercent = ((node.usedBytes + node.reservedBytes) / node.capacityBytes) * 100;
  return utilizationPercent >= highWatermarkPercent ? "OVER_CAPACITY" : null;
}
