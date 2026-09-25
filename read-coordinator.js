import { createHash } from "node:crypto";
import { PassThrough } from "node:stream";

import { ConsistencyError } from "./errors.js";

/**
 * Reads immutable manifest versions with replica-level quorum verification.
 * Successful reads enqueue repair hints for bad, unavailable, or stale
 * replicas; copying is deliberately delegated to the repair worker.
 */
export class ReadCoordinator {
  #metadata;
  #transport;
  #repairQueue;

  constructor({ metadata, transport, repairQueue }) {
    if (!metadata || typeof metadata.getManifest !== "function" || typeof metadata.getNode !== "function" || typeof metadata.getDurabilityPolicy !== "function") {
      throw new TypeError("metadata must support manifests, nodes, and policies");
    }
    if (!transport || typeof transport.getChunk !== "function") throw new TypeError("transport must support getChunk()");
    if (!repairQueue || typeof repairQueue.enqueue !== "function") throw new TypeError("repairQueue must support enqueue()");
    this.#metadata = metadata;
    this.#transport = transport;
    this.#repairQueue = repairQueue;
  }

  async getObject(objectId, { version } = {}) {
    const manifest = this.#metadata.getManifest(objectId, { version });
    const policy = this.#metadata.getDurabilityPolicy(manifest.metadata.policyId ?? "standard");
    const output = new PassThrough();
    const completion = this.#stream(manifest, policy, output);
    completion.catch((error) => output.destroy(error));
    return Object.freeze({ objectId: manifest.objectId, version: manifest.version, sizeBytes: manifest.sizeBytes, manifest, stream: output, completion });
  }

  async #stream(manifest, policy, output) {
    const objectHash = createHash("sha256");
    try {
      for (const chunk of manifest.chunks) {
        const bytes = await this.#readChunk(manifest, chunk, policy.readQuorum);
        objectHash.update(bytes);
        if (!output.write(bytes)) await onceDrain(output);
      }
      const observed = objectHash.digest("hex");
      if (observed !== manifest.contentChecksum.value) {
        throw new ConsistencyError("OBJECT_CHECKSUM_MISMATCH", `Object ${manifest.objectId} does not match its manifest checksum`);
      }
      output.end();
    } catch (error) {
      output.destroy(error);
      throw error;
    }
  }

  async #readChunk(manifest, chunk, requiredReadQuorum) {
    const pending = [];
    const failures = [];
    for (const replica of chunk.replicas) {
      if (replica.state !== "healthy") {
        failures.push({ nodeId: replica.nodeId, reason: "REPLICA_MARKED_STALE" });
        continue;
      }
      try {
        const node = this.#metadata.getNode(replica.nodeId);
        pending.push(readAndVerify(this.#transport, node, chunk)
          .then((bytes) => ({ ok: true, nodeId: replica.nodeId, bytes }))
          .catch((error) => ({ ok: false, nodeId: replica.nodeId, error })));
      } catch (error) {
        failures.push({ nodeId: replica.nodeId, reason: error?.code ?? "NODE_NOT_FOUND" });
      }
    }

    const verified = [];
    while (pending.length > 0) {
      const outcome = await Promise.race(pending.map((promise, index) => promise.then((result) => ({ ...result, index }))));
      pending.splice(outcome.index, 1);
      if (outcome.ok) {
        verified.push(outcome);
        if (verified.length >= requiredReadQuorum) {
          this.#enqueueRepairs(manifest, chunk, verified[0].nodeId, failures);
          // Do not delay a successful caller for slower replicas. Their
          // outcomes are still observed and converted to repair work.
          void Promise.all(pending).then((laterOutcomes) => {
            const laterFailures = laterOutcomes
              .filter((result) => !result.ok)
              .map((result) => ({ nodeId: result.nodeId, reason: normalizeReadFailure(result.error) }));
            this.#enqueueRepairs(manifest, chunk, verified[0].nodeId, laterFailures);
          });
          return verified[0].bytes;
        }
      } else {
        failures.push({ nodeId: outcome.nodeId, reason: normalizeReadFailure(outcome.error) });
      }
      if (verified.length + pending.length < requiredReadQuorum) break;
    }
    // The caller cannot receive this chunk, but a verified source can still
    // repair replicas. Finish observing outstanding reads before reporting
    // the failed quorum so no valid source is discarded.
    for (const outcome of await Promise.all(pending)) {
      if (outcome.ok) verified.push(outcome);
      else failures.push({ nodeId: outcome.nodeId, reason: normalizeReadFailure(outcome.error) });
    }
    if (verified.length > 0) this.#enqueueRepairs(manifest, chunk, verified[0].nodeId, failures);
    throw new ConsistencyError(
      "READ_QUORUM_UNAVAILABLE",
      `Chunk ${chunk.chunkId} did not receive ${requiredReadQuorum} verified responses`,
      { details: { chunkId: chunk.chunkId, requiredReadQuorum, failures } }
    );
  }

  #enqueueRepairs(manifest, chunk, sourceNodeId, failures) {
    for (const failure of failures) {
      this.#repairQueue.enqueue({
        objectId: manifest.objectId,
        version: manifest.version,
        chunk,
        sourceNodeId,
        targetNodeId: failure.nodeId,
        reasons: [failure.reason],
        priority: repairPriority(failure.reason),
        observedAt: new Date().toISOString()
      });
    }
  }
}

async function readAndVerify(transport, node, chunk) {
  const result = await transport.getChunk(node, chunk.chunkId);
  if (result.sizeBytes !== chunk.sizeBytes) {
    result.stream?.resume?.();
    throw new ConsistencyError("CHUNK_LENGTH_MISMATCH", `Node ${node.nodeId} returned an incorrect length`);
  }
  const hash = createHash("sha256");
  const buffers = [];
  for await (const bytes of result.stream) {
    hash.update(bytes);
    buffers.push(bytes);
  }
  if (hash.digest("hex") !== chunk.checksum.value) {
    throw new ConsistencyError("CHUNK_CHECKSUM_MISMATCH", `Node ${node.nodeId} returned corrupt bytes`);
  }
  return Buffer.concat(buffers);
}

function normalizeReadFailure(error) {
  if (error?.code === "CHUNK_CHECKSUM_MISMATCH" || error?.code === "CORRUPT_CHUNK") return "CHECKSUM_MISMATCH";
  if (error?.code === "CHUNK_LENGTH_MISMATCH") return "LENGTH_MISMATCH";
  if (["CHUNK_NOT_FOUND", "NODE_NOT_FOUND"].includes(error?.code)) return "MISSING";
  if (["NODE_UNAVAILABLE", "NODE_UNREACHABLE", "NODE_TIMEOUT"].includes(error?.code)) return "UNAVAILABLE";
  return "READ_FAILED";
}

function repairPriority(reason) {
  return ({ CHECKSUM_MISMATCH: 100, MISSING: 90, LENGTH_MISMATCH: 90, UNAVAILABLE: 60, REPLICA_MARKED_STALE: 50, READ_FAILED: 40 })[reason] ?? 20;
}

function onceDrain(stream) {
  return new Promise((resolve, reject) => {
    stream.once("drain", resolve);
    stream.once("error", reject);
  });
}
