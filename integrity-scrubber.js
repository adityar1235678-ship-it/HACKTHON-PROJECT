import { createHash } from "node:crypto";

import { RepairError } from "./errors.js";

/**
 * Verifies stored replicas against their immutable chunk content address and
 * turns deficits into coalesced repair work. It is safe to run repeatedly.
 */
export class IntegrityScrubber {
  #metadata;
  #placement;
  #transport;
  #repairQueue;

  constructor({ metadata, placement, transport, repairQueue }) {
    if (!metadata || typeof metadata.listManifests !== "function" || typeof metadata.getReplicaSet !== "function" || typeof metadata.updateReplicaState !== "function" || typeof metadata.getNode !== "function") {
      throw new TypeError("metadata must support manifests, replica state, and node lookup");
    }
    if (!placement || typeof placement.planWrite !== "function") throw new TypeError("placement must support planWrite()");
    if (!transport || typeof transport.getChunk !== "function") throw new TypeError("transport must support getChunk()");
    if (!repairQueue || typeof repairQueue.enqueue !== "function") throw new TypeError("repairQueue must support enqueue()");
    this.#metadata = metadata;
    this.#placement = placement;
    this.#transport = transport;
    this.#repairQueue = repairQueue;
  }

  async scrub({ maxChunks = Number.MAX_SAFE_INTEGER } = {}) {
    if (!Number.isSafeInteger(maxChunks) || maxChunks < 0) throw new TypeError("maxChunks must be a non-negative safe integer");
    const summary = { scannedChunks: 0, checkedReplicas: 0, verifiedReplicas: 0, failedReplicas: 0, enqueuedRepairs: 0, unrecoverableChunks: 0 };
    for (const manifest of this.#metadata.listManifests()) {
      if (summary.scannedChunks >= maxChunks) break;
      for (const chunk of manifest.chunks) {
        if (summary.scannedChunks >= maxChunks) break;
        summary.scannedChunks += 1;
        const result = await this.#scrubChunk(manifest, chunk);
        summary.checkedReplicas += result.checkedReplicas;
        summary.verifiedReplicas += result.verifiedReplicas;
        summary.failedReplicas += result.failedReplicas;
        summary.enqueuedRepairs += result.enqueuedRepairs;
        summary.unrecoverableChunks += result.unrecoverable ? 1 : 0;
      }
    }
    return Object.freeze(summary);
  }

  async #scrubChunk(manifest, chunk) {
    const replicaSet = this.#metadata.getReplicaSet(manifest.objectId, { version: manifest.version, chunkId: chunk.chunkId });
    const verifiedNodeIds = [];
    let checkedReplicas = 0;
    let failedReplicas = 0;
    let enqueuedRepairs = 0;

    for (const replica of replicaSet.replicas) {
      if (replica.state === "unavailable") continue;
      checkedReplicas += 1;
      try {
        const node = this.#metadata.getNode(replica.nodeId);
        await readAndVerify(this.#transport, node, chunk);
        verifiedNodeIds.push(replica.nodeId);
        if (replica.state !== "healthy" || replica.reason !== null) {
          await this.#metadata.updateReplicaState(manifest.objectId, {
            version: manifest.version, chunkId: chunk.chunkId, nodeId: replica.nodeId, state: "healthy", reason: null
          });
        }
      } catch (error) {
        failedReplicas += 1;
        await this.#metadata.updateReplicaState(manifest.objectId, {
          version: manifest.version,
          chunkId: chunk.chunkId,
          nodeId: replica.nodeId,
          state: stateForFailure(error),
          reason: reasonForFailure(error)
        });
      }
    }

    if (verifiedNodeIds.length === 0) {
      return { checkedReplicas, verifiedReplicas: 0, failedReplicas, enqueuedRepairs, unrecoverable: true };
    }
    try {
      const plan = this.#placement.planWrite({
        chunkId: chunk.chunkId,
        sizeBytes: chunk.sizeBytes,
        policyId: manifest.metadata.policyId ?? "standard",
        existingReplicaNodeIds: verifiedNodeIds,
        allowDegraded: true
      });
      for (const target of plan.writeTargets) {
        this.#repairQueue.enqueue({
          objectId: manifest.objectId,
          version: manifest.version,
          chunk,
          sourceNodeId: verifiedNodeIds[0],
          targetNodeId: target.nodeId,
          reasons: ["SCRUB_REPLICATION_DEFICIT"],
          priority: plan.replicaDeficit > 0 ? 85 : 70,
          observedAt: new Date().toISOString()
        });
        enqueuedRepairs += 1;
      }
      return { checkedReplicas, verifiedReplicas: verifiedNodeIds.length, failedReplicas, enqueuedRepairs, unrecoverable: false };
    } catch (error) {
      if (error?.code === "WRITE_QUORUM_UNAVAILABLE" || error?.code === "REPLICATION_UNSATISFIABLE") {
        return { checkedReplicas, verifiedReplicas: verifiedNodeIds.length, failedReplicas, enqueuedRepairs, unrecoverable: true };
      }
      throw error;
    }
  }
}

async function readAndVerify(transport, node, chunk) {
  const result = await transport.getChunk(node, chunk.chunkId);
  if (result.sizeBytes !== chunk.sizeBytes) {
    result.stream?.resume?.();
    throw new RepairError("CHUNK_LENGTH_MISMATCH", `Node ${node.nodeId} returned an invalid length`);
  }
  const hash = createHash("sha256");
  for await (const bytes of result.stream) hash.update(bytes);
  if (hash.digest("hex") !== chunk.checksum.value) throw new RepairError("CHUNK_CHECKSUM_MISMATCH", `Node ${node.nodeId} returned invalid bytes`);
}

function stateForFailure(error) {
  return ["NODE_UNAVAILABLE", "NODE_UNREACHABLE", "NODE_TIMEOUT"].includes(error?.code) ? "unavailable" : "stale";
}

function reasonForFailure(error) {
  if (["CHUNK_CHECKSUM_MISMATCH", "CORRUPT_CHUNK"].includes(error?.code)) return "CHECKSUM_MISMATCH";
  if (error?.code === "CHUNK_LENGTH_MISMATCH") return "LENGTH_MISMATCH";
  if (error?.code === "CHUNK_NOT_FOUND") return "MISSING";
  if (["NODE_UNAVAILABLE", "NODE_UNREACHABLE", "NODE_TIMEOUT"].includes(error?.code)) return "UNAVAILABLE";
  return "SCRUB_FAILED";
}
