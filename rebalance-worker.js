import { createHash } from "node:crypto";

import { RebalanceError } from "./errors.js";

/** Copies first, confirms, then retires the old placement from the overlay. */
export class RebalanceWorker {
  #metadata;
  #placement;
  #transport;
  #queue;

  constructor({ metadata, placement, transport, queue }) {
    if (!metadata || typeof metadata.getManifest !== "function" || typeof metadata.getReplicaSet !== "function" || typeof metadata.updateReplicaState !== "function" || typeof metadata.getNode !== "function") {
      throw new TypeError("metadata must support manifests, replica state, and node lookup");
    }
    if (!placement || typeof placement.planWrite !== "function") throw new TypeError("placement must support planWrite()");
    if (!transport || typeof transport.getChunk !== "function" || typeof transport.putChunk !== "function") {
      throw new TypeError("transport must support getChunk() and putChunk()");
    }
    if (!queue || typeof queue.claim !== "function" || typeof queue.enqueue !== "function") throw new TypeError("queue must support claim() and enqueue()");
    this.#metadata = metadata;
    this.#placement = placement;
    this.#transport = transport;
    this.#queue = queue;
  }

  async runOnce() {
    const job = this.#queue.claim();
    if (!job) return Object.freeze({ status: "idle" });
    try {
      return freezeClone(await this.#move(job));
    } catch (error) {
      this.#queue.enqueue({
        objectId: job.objectId,
        version: job.version,
        chunk: job.chunk,
        sourceNodeId: job.sourceNodeId,
        targetNodeId: job.targetNodeId,
        reasons: [...job.reasons, "RETRY"],
        priority: Math.max(1, job.priority - 5),
        observedAt: new Date().toISOString()
      });
      return freezeClone({ status: "deferred", job, error: { code: error?.code ?? "REBALANCE_FAILED", message: error?.message ?? "Rebalance failed" } });
    }
  }

  async runBatch({ maxJobs = 100 } = {}) {
    if (!Number.isSafeInteger(maxJobs) || maxJobs < 0) throw new TypeError("maxJobs must be a non-negative safe integer");
    const results = [];
    for (let index = 0; index < maxJobs; index += 1) {
      const result = await this.runOnce();
      if (result.status === "idle") break;
      results.push(result);
      if (result.status === "deferred") break;
    }
    return Object.freeze(results);
  }

  async #move(job) {
    const manifest = this.#metadata.getManifest(job.objectId, { version: job.version });
    const chunk = manifest.chunks.find((candidate) => candidate.chunkId === job.chunk.chunkId);
    if (!chunk) throw new RebalanceError("CHUNK_NOT_FOUND", `Chunk ${job.chunk.chunkId} is absent from manifest`);
    const replicaSet = this.#metadata.getReplicaSet(job.objectId, { version: job.version, chunkId: chunk.chunkId });
    const sourceReplica = replicaSet.replicas.find((replica) => replica.nodeId === job.sourceNodeId && replica.state === "healthy");
    if (!sourceReplica) return { status: "source-not-healthy", objectId: job.objectId, version: job.version, chunkId: chunk.chunkId };
    const sourceNode = this.#metadata.getNode(job.sourceNodeId);
    const bytes = await readAndVerify(this.#transport, sourceNode, chunk);
    const existingReplicaNodeIds = replicaSet.replicas
      .filter((replica) => replica.state === "healthy" && replica.nodeId !== job.sourceNodeId)
      .map((replica) => replica.nodeId);
    const plan = this.#placement.planWrite({
      chunkId: chunk.chunkId,
      sizeBytes: chunk.sizeBytes,
      policyId: manifest.metadata.policyId ?? "standard",
      existingReplicaNodeIds,
      excludedNodeIds: [job.sourceNodeId],
      allowDegraded: false
    });
    const target = job.targetNodeId
      ? plan.writeTargets.find((candidate) => candidate.nodeId === job.targetNodeId)
      : plan.writeTargets[0];
    if (!target) throw new RebalanceError("NO_REBALANCE_TARGET", `No safe target is available for ${chunk.chunkId}`);

    await this.#metadata.updateReplicaState(job.objectId, {
      version: job.version, chunkId: chunk.chunkId, nodeId: target.nodeId, state: "repairing", reason: "REBALANCE_IN_PROGRESS"
    });
    const acknowledgement = await this.#transport.putChunk(target, chunk.chunkId, bytes);
    if (acknowledgement.nodeId !== target.nodeId) throw new RebalanceError("INVALID_REBALANCE_ACK", `Unexpected acknowledgement from ${acknowledgement.nodeId}`);
    await this.#metadata.updateReplicaState(job.objectId, {
      version: job.version, chunkId: chunk.chunkId, nodeId: target.nodeId, state: "healthy", reason: null
    });
    // Overlay retirement is deliberately after the new durable copy is known.
    await this.#metadata.updateReplicaState(job.objectId, {
      version: job.version, chunkId: chunk.chunkId, nodeId: job.sourceNodeId, state: "stale", reason: "REBALANCED"
    });
    return {
      status: "moved",
      objectId: job.objectId,
      version: job.version,
      chunkId: chunk.chunkId,
      sourceNodeId: job.sourceNodeId,
      targetNodeId: target.nodeId
    };
  }
}

async function readAndVerify(transport, node, chunk) {
  const result = await transport.getChunk(node, chunk.chunkId);
  if (result.sizeBytes !== chunk.sizeBytes) {
    result.stream?.resume?.();
    throw new RebalanceError("CHUNK_LENGTH_MISMATCH", `Node ${node.nodeId} returned an invalid length`);
  }
  const hash = createHash("sha256");
  const buffers = [];
  for await (const bytes of result.stream) {
    hash.update(bytes);
    buffers.push(bytes);
  }
  if (hash.digest("hex") !== chunk.checksum.value) throw new RebalanceError("CHUNK_CHECKSUM_MISMATCH", `Node ${node.nodeId} returned invalid bytes`);
  return Buffer.concat(buffers);
}

function freezeClone(value) {
  return deepFreeze(structuredClone(value));
}

function deepFreeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}
