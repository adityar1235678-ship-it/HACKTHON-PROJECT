import { createHash } from "node:crypto";

import { RepairError } from "./errors.js";

/** Executes one coalesced repair job at a time, keeping foreground reads free. */
export class RepairWorker {
  #metadata;
  #placement;
  #transport;
  #repairQueue;

  constructor({ metadata, placement, transport, repairQueue }) {
    if (!metadata || typeof metadata.getManifest !== "function" || typeof metadata.getReplicaSet !== "function" || typeof metadata.updateReplicaState !== "function" || typeof metadata.getNode !== "function") {
      throw new TypeError("metadata must support manifests, replica state, and node lookup");
    }
    if (!placement || typeof placement.planWrite !== "function") throw new TypeError("placement must support planWrite()");
    if (!transport || typeof transport.getChunk !== "function" || typeof transport.putChunk !== "function") {
      throw new TypeError("transport must support getChunk() and putChunk()");
    }
    if (!repairQueue || typeof repairQueue.claim !== "function" || typeof repairQueue.enqueue !== "function") throw new TypeError("repairQueue must support claim() and enqueue()");
    this.#metadata = metadata;
    this.#placement = placement;
    this.#transport = transport;
    this.#repairQueue = repairQueue;
  }

  async runOnce() {
    const job = this.#repairQueue.claim();
    if (!job) return Object.freeze({ status: "idle" });
    try {
      return freezeClone(await this.#repair(job));
    } catch (error) {
      await this.#markTargetFailure(job, error);
      this.#repairQueue.enqueue({
        objectId: job.objectId,
        version: job.version,
        chunk: job.chunk,
        sourceNodeId: job.sourceNodeId,
        targetNodeId: job.targetNodeId,
        reasons: [...job.reasons, "RETRY"],
        priority: Math.max(1, job.priority - 5),
        observedAt: new Date().toISOString()
      });
      return freezeClone({ status: "deferred", job, error: { code: error?.code ?? "REPAIR_FAILED", message: error?.message ?? "Repair failed" } });
    }
  }

  /** Process a bounded repair batch; deferred work is left for a later pass. */
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

  async #repair(job) {
    const manifest = this.#metadata.getManifest(job.objectId, { version: job.version });
    const chunk = manifest.chunks.find((candidate) => candidate.chunkId === job.chunk.chunkId);
    if (!chunk) throw new RepairError("CHUNK_NOT_FOUND", `Repair job chunk ${job.chunk.chunkId} is absent from manifest`);
    const source = await this.#findVerifiedSource(manifest, chunk, job.targetNodeId, job.sourceNodeId);
    const replicaSet = this.#metadata.getReplicaSet(job.objectId, { version: job.version, chunkId: chunk.chunkId });
    const existingReplicaNodeIds = replicaSet.replicas
      .filter((replica) => replica.state === "healthy" && replica.nodeId !== job.targetNodeId)
      .map((replica) => replica.nodeId);
    const plan = this.#placement.planWrite({
      chunkId: chunk.chunkId,
      sizeBytes: chunk.sizeBytes,
      policyId: manifest.metadata.policyId ?? "standard",
      existingReplicaNodeIds,
      allowDegraded: true
    });
    const target = plan.writeTargets.find((candidate) => candidate.nodeId === job.targetNodeId) ?? plan.writeTargets[0];
    if (!target) {
      return { status: "already-durable", objectId: job.objectId, version: job.version, chunkId: chunk.chunkId };
    }

    await this.#metadata.updateReplicaState(job.objectId, {
      version: job.version, chunkId: chunk.chunkId, nodeId: target.nodeId, state: "repairing", reason: "REPAIR_IN_PROGRESS"
    });
    const acknowledgement = target.nodeId === job.targetNodeId && typeof this.#transport.replaceChunk === "function"
      ? await this.#transport.replaceChunk(target, chunk.chunkId, source.bytes)
      : await this.#transport.putChunk(target, chunk.chunkId, source.bytes);
    if (acknowledgement.nodeId !== target.nodeId) throw new RepairError("INVALID_REPAIR_ACK", `Unexpected acknowledgement from ${acknowledgement.nodeId}`);
    await this.#metadata.updateReplicaState(job.objectId, {
      version: job.version, chunkId: chunk.chunkId, nodeId: target.nodeId, state: "healthy", reason: null
    });
    return {
      status: "repaired",
      objectId: job.objectId,
      version: job.version,
      chunkId: chunk.chunkId,
      sourceNodeId: source.nodeId,
      targetNodeId: target.nodeId,
      replicaDeficit: plan.replicaDeficit
    };
  }

  async #findVerifiedSource(manifest, chunk, targetNodeId, preferredSourceNodeId) {
    const replicaSet = this.#metadata.getReplicaSet(manifest.objectId, { version: manifest.version, chunkId: chunk.chunkId });
    const sourceIds = [preferredSourceNodeId, ...replicaSet.replicas.map((replica) => replica.nodeId)]
      .filter((nodeId, index, list) => nodeId !== targetNodeId && list.indexOf(nodeId) === index);
    for (const nodeId of sourceIds) {
      try {
        const node = this.#metadata.getNode(nodeId);
        const bytes = await readAndVerify(this.#transport, node, chunk);
        await this.#metadata.updateReplicaState(manifest.objectId, {
          version: manifest.version, chunkId: chunk.chunkId, nodeId, state: "healthy", reason: null
        });
        return { nodeId, bytes };
      } catch (error) {
        await this.#markReplicaFailure(manifest, chunk, nodeId, error);
      }
    }
    throw new RepairError("NO_VERIFIED_REPAIR_SOURCE", `No verified source exists for ${chunk.chunkId}`);
  }

  async #markTargetFailure(job, error) {
    await this.#markReplicaFailure({ objectId: job.objectId, version: job.version }, job.chunk, job.targetNodeId, error);
  }

  async #markReplicaFailure(manifest, chunk, nodeId, error) {
    try {
      await this.#metadata.updateReplicaState(manifest.objectId, {
        version: manifest.version,
        chunkId: chunk.chunkId,
        nodeId,
        state: ["NODE_UNAVAILABLE", "NODE_UNREACHABLE", "NODE_TIMEOUT"].includes(error?.code) ? "unavailable" : "stale",
        reason: error?.code ?? "REPAIR_FAILED"
      });
    } catch {
      // A removed/missing metadata record should not hide the original repair failure.
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
  const buffers = [];
  for await (const bytes of result.stream) {
    hash.update(bytes);
    buffers.push(bytes);
  }
  if (hash.digest("hex") !== chunk.checksum.value) throw new RepairError("CHUNK_CHECKSUM_MISMATCH", `Node ${node.nodeId} returned invalid bytes`);
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
