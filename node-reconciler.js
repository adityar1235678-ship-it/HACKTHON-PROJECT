import { createHash } from "node:crypto";

import { RecoveryError } from "./errors.js";
import { activeReferencesOnNode, buildChunkReferenceIndex, repairSourceFor } from "./reference-index.js";

/**
 * Anti-entropy inventory comparison for a storage node that has rejoined or
 * may have diverged during a partial partition. It compares actual chunk IDs
 * and verified bytes to metadata's dynamic replica overlay.
 */
export class NodeReconciler {
  #metadata;
  #transport;
  #repairQueue;

  constructor({ metadata, transport, repairQueue }) {
    if (!metadata || typeof metadata.listManifests !== "function" || typeof metadata.getReplicaSet !== "function" || typeof metadata.updateReplicaState !== "function" || typeof metadata.getNode !== "function" || typeof metadata.observeOrphanCandidate !== "function") {
      throw new TypeError("metadata must support manifests, replica state, node lookup, and orphan observations");
    }
    if (!transport || typeof transport.listChunkIds !== "function" || typeof transport.getChunk !== "function") {
      throw new TypeError("transport must support listChunkIds() and getChunk()");
    }
    if (!repairQueue || typeof repairQueue.enqueue !== "function") throw new TypeError("repairQueue must support enqueue()");
    this.#metadata = metadata;
    this.#transport = transport;
    this.#repairQueue = repairQueue;
  }

  async reconcileNode(nodeId) {
    const node = this.#metadata.getNode(nodeId);
    let inventory;
    try {
      inventory = new Set(await this.#transport.listChunkIds(node));
    } catch (error) {
      return Object.freeze({ nodeId, status: "unreachable", error: { code: error?.code ?? "INVENTORY_FAILED", message: error?.message ?? "Inventory request failed" } });
    }
    const referencesByChunkId = buildChunkReferenceIndex(this.#metadata);
    const summary = {
      nodeId,
      status: "reconciled",
      inventoryChunkCount: inventory.size,
      expectedChunks: 0,
      recoveredReplicas: 0,
      missingReplicas: 0,
      corruptReplicas: 0,
      repairJobs: 0,
      orphanCandidatesObserved: 0
    };

    for (const [chunkId, references] of referencesByChunkId) {
      const expected = activeReferencesOnNode(references, nodeId);
      if (expected.length === 0) continue;
      summary.expectedChunks += 1;
      if (!inventory.has(chunkId)) {
        for (const reference of expected) {
          const queued = await this.#markAndQueue(reference, nodeId, "REJOIN_MISSING", "stale");
          summary.missingReplicas += 1;
          if (queued) summary.repairJobs += 1;
        }
        continue;
      }
      try {
        await verifyChunk(this.#transport, node, expected[0].chunk);
        for (const reference of expected) {
          const prior = reference.replicas.find((replica) => replica.nodeId === nodeId);
          if (prior?.state !== "healthy" || prior?.reason !== null) {
            await this.#metadata.updateReplicaState(reference.objectId, {
              version: reference.version, chunkId, nodeId, state: "healthy", reason: null
            });
            summary.recoveredReplicas += 1;
          }
        }
      } catch (error) {
        for (const reference of expected) {
          const queued = await this.#markAndQueue(reference, nodeId, "REJOIN_CORRUPT", "stale", error);
          summary.corruptReplicas += 1;
          if (queued) summary.repairJobs += 1;
        }
      }
    }

    for (const chunkId of inventory) {
      if (activeReferencesOnNode(referencesByChunkId.get(chunkId), nodeId).length === 0) {
        await this.#metadata.observeOrphanCandidate(nodeId, chunkId);
        summary.orphanCandidatesObserved += 1;
      }
    }
    return Object.freeze(summary);
  }

  async antiEntropy({ nodeIds, maxNodes = Number.MAX_SAFE_INTEGER } = {}) {
    if (nodeIds !== undefined && !Array.isArray(nodeIds)) throw new TypeError("nodeIds must be an array when provided");
    if (!Number.isSafeInteger(maxNodes) || maxNodes < 0) throw new TypeError("maxNodes must be a non-negative safe integer");
    const selected = nodeIds ?? this.#metadata.listNodes({ includeRemoved: false }).map((node) => node.nodeId);
    const results = [];
    for (const nodeId of selected.slice(0, maxNodes)) results.push(await this.reconcileNode(nodeId));
    return Object.freeze(results);
  }

  async #markAndQueue(reference, targetNodeId, reason, state, error) {
    await this.#metadata.updateReplicaState(reference.objectId, {
      version: reference.version,
      chunkId: reference.chunk.chunkId,
      nodeId: targetNodeId,
      state,
      reason
    });
    const sourceNodeId = repairSourceFor(reference, targetNodeId);
    if (!sourceNodeId) return false;
    this.#repairQueue.enqueue({
      objectId: reference.objectId,
      version: reference.version,
      chunk: reference.chunk,
      sourceNodeId,
      targetNodeId,
      reasons: [reason, error?.code].filter(Boolean),
      priority: reason === "REJOIN_CORRUPT" ? 100 : 90,
      observedAt: new Date().toISOString()
    });
    return true;
  }
}

async function verifyChunk(transport, node, chunk) {
  const result = await transport.getChunk(node, chunk.chunkId);
  if (result.sizeBytes !== chunk.sizeBytes) {
    result.stream?.resume?.();
    throw new RecoveryError("CHUNK_LENGTH_MISMATCH", `Node ${node.nodeId} returned an invalid length`);
  }
  const hash = createHash("sha256");
  for await (const bytes of result.stream) hash.update(bytes);
  if (hash.digest("hex") !== chunk.checksum.value) throw new RecoveryError("CHUNK_CHECKSUM_MISMATCH", `Node ${node.nodeId} returned invalid bytes`);
}
