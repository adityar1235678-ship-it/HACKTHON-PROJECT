import { buildChunkReferenceIndex, hasActivePlacementOnNode } from "./reference-index.js";

/**
 * Conservative two-observation orphan cleanup. A local copy is removed only
 * after the grace window and only when every manifest using those bytes still
 * has its full required durable replica set without this node.
 */
export class OrphanCleaner {
  #metadata;
  #placement;
  #transport;
  #gracePeriodMs;
  #clock;

  constructor({ metadata, placement, transport, gracePeriodMs = 60 * 60 * 1000, clock = () => Date.now() }) {
    if (!metadata || typeof metadata.listManifests !== "function" || typeof metadata.getReplicaSet !== "function" || typeof metadata.getNode !== "function" || typeof metadata.observeOrphanCandidate !== "function" || typeof metadata.listOrphanCandidates !== "function" || typeof metadata.clearOrphanCandidate !== "function") {
      throw new TypeError("metadata must support manifests, replica state, nodes, and orphan candidates");
    }
    if (!placement || typeof placement.assessReplicaSet !== "function") throw new TypeError("placement must support assessReplicaSet()");
    if (!transport || typeof transport.listChunkIds !== "function" || typeof transport.deleteChunk !== "function") {
      throw new TypeError("transport must support listChunkIds() and deleteChunk()");
    }
    if (!Number.isSafeInteger(gracePeriodMs) || gracePeriodMs < 0) throw new TypeError("gracePeriodMs must be a non-negative safe integer");
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    this.#metadata = metadata;
    this.#placement = placement;
    this.#transport = transport;
    this.#gracePeriodMs = gracePeriodMs;
    this.#clock = clock;
  }

  async scanNode(nodeId) {
    const node = this.#metadata.getNode(nodeId);
    let inventory;
    try {
      inventory = new Set(await this.#transport.listChunkIds(node));
    } catch (error) {
      return Object.freeze({ nodeId, status: "unreachable", error: { code: error?.code ?? "INVENTORY_FAILED", message: error?.message ?? "Inventory request failed" } });
    }
    const referencesByChunkId = buildChunkReferenceIndex(this.#metadata);
    const summary = { nodeId, status: "scanned", observed: 0, retained: 0, deleted: 0, clearedMissing: 0 };

    for (const candidate of this.#metadata.listOrphanCandidates({ nodeId })) {
      if (!inventory.has(candidate.chunkId)) {
        await this.#metadata.clearOrphanCandidate(nodeId, candidate.chunkId);
        summary.clearedMissing += 1;
      }
    }
    for (const chunkId of inventory) {
      const references = referencesByChunkId.get(chunkId);
      if (hasActivePlacementOnNode(references, nodeId)) {
        const prior = this.#metadata.getOrphanCandidate(nodeId, chunkId);
        if (prior) await this.#metadata.clearOrphanCandidate(nodeId, chunkId);
        continue;
      }
      const candidate = await this.#metadata.observeOrphanCandidate(nodeId, chunkId);
      summary.observed += 1;
      if (!this.#eligible(candidate) || !this.#safeToDelete(references, nodeId)) {
        summary.retained += 1;
        continue;
      }
      const result = await this.#transport.deleteChunk(node, chunkId);
      if (result.deleted) summary.deleted += 1;
      await this.#metadata.clearOrphanCandidate(nodeId, chunkId);
    }
    return Object.freeze(summary);
  }

  #eligible(candidate) {
    return candidate.observations >= 2 && this.#clock() - Date.parse(candidate.firstObservedAt) >= this.#gracePeriodMs;
  }

  #safeToDelete(references, nodeId) {
    if (!references || references.length === 0) return true;
    return references.every((reference) => {
      const otherHealthyNodeIds = reference.replicas
        .filter((replica) => replica.nodeId !== nodeId && replica.state === "healthy")
        .map((replica) => replica.nodeId);
      const assessment = this.#placement.assessReplicaSet({ policyId: reference.policyId, replicaNodeIds: otherHealthyNodeIds });
      return assessment.replicaDeficit === 0;
    });
  }
}
