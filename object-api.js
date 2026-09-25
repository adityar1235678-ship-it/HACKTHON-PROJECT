import { createHash, randomUUID } from "node:crypto";
import { PassThrough } from "node:stream";

import { fixedSizeChunks } from "../core/chunker.js";
import { checksumFor } from "../core/checksum.js";
import { canonicalJson, MANIFEST_SCHEMA_VERSION, newObjectId, newVersionId, validateManifest } from "../core/manifest.js";
import { ObjectApiError } from "./errors.js";

const DEFAULT_CHUNK_SIZE_BYTES = 8 * 1024 * 1024;

/**
 * Coordinates object writes and reads. It owns no durable bytes: storage nodes
 * own chunks and MetadataStore atomically owns the object-version pointer.
 */
export class ObjectApi {
  #metadata;
  #placement;
  #transport;
  #chunkSizeBytes;
  #idempotency = new Map();
  #objectLocks = new Map();

  constructor({ metadata, placement, transport, chunkSizeBytes = DEFAULT_CHUNK_SIZE_BYTES }) {
    if (!metadata || typeof metadata.publishManifest !== "function" || typeof metadata.getManifest !== "function" ||
      typeof metadata.getDurabilityPolicy !== "function" || typeof metadata.getNode !== "function") {
      throw new TypeError("metadata must support manifest, policy, and node retrieval");
    }
    if (!placement || typeof placement.planWrite !== "function") throw new TypeError("placement must support planWrite()");
    if (!transport || typeof transport.putChunk !== "function" || typeof transport.getChunk !== "function") {
      throw new TypeError("transport must support putChunk() and getChunk()");
    }
    if (!Number.isSafeInteger(chunkSizeBytes) || chunkSizeBytes <= 0) throw new TypeError("chunkSizeBytes must be a positive safe integer");
    this.#metadata = metadata;
    this.#placement = placement;
    this.#transport = transport;
    this.#chunkSizeBytes = chunkSizeBytes;
  }

  /**
   * Write an object stream. All chunks must meet their policy write quorum
   * before the manifest is atomically published as the current object version.
   */
  async putObject(input, {
    objectId = newObjectId(),
    version = newVersionId(),
    policyId = "standard",
    expectedCurrentVersion = null,
    metadata = {},
    allowDegraded = false,
    idempotencyKey
  } = {}) {
    assertObjectId(objectId);
    assertVersion(version);
    assertExpectedVersion(expectedCurrentVersion);
    assertMetadata(metadata);
    if (typeof allowDegraded !== "boolean") throw new TypeError("allowDegraded must be a boolean");
    if (idempotencyKey !== undefined) assertIdempotencyKey(idempotencyKey);

    const requestFingerprint = fingerprint({ objectId, version, policyId, expectedCurrentVersion, metadata, allowDegraded });
    if (idempotencyKey) {
      const previous = this.#idempotency.get(idempotencyKey);
      if (previous) {
        if (previous.fingerprint !== requestFingerprint) throw new ObjectApiError("IDEMPOTENCY_CONFLICT", "idempotencyKey was already used with different request parameters");
        return previous.promise;
      }
    }

    const operation = this.#withObjectLock(objectId, () => this.#putObject(input, {
      objectId, version, policyId, expectedCurrentVersion, metadata, allowDegraded
    }));
    if (idempotencyKey) {
      const tracked = { fingerprint: requestFingerprint, promise: operation };
      this.#idempotency.set(idempotencyKey, tracked);
      operation.catch(() => {
        if (this.#idempotency.get(idempotencyKey) === tracked) this.#idempotency.delete(idempotencyKey);
      });
    }
    return operation;
  }

  async #putObject(input, options) {
    const contentHash = createHash("sha256");
    const chunks = [];
    let sizeBytes = 0;

    for await (const bytes of fixedSizeChunks(input, this.#chunkSizeBytes)) {
      contentHash.update(bytes);
      sizeBytes += bytes.length;
      const checksum = checksumFor(bytes);
      const chunkId = `chk_${checksum.value}`;
      const plan = this.#placement.planWrite({
        chunkId,
        sizeBytes: bytes.length,
        policyId: options.policyId,
        allowDegraded: options.allowDegraded
      });
      const replicas = await writeQuorum(this.#transport, plan, chunkId, bytes);
      chunks.push({ index: chunks.length, chunkId, sizeBytes: bytes.length, checksum, replicas });
    }

    const contentChecksum = { algorithm: "sha256", value: contentHash.digest("hex") };
    const manifestContent = {
      schemaVersion: MANIFEST_SCHEMA_VERSION,
      objectId: options.objectId,
      version: options.version,
      createdAt: new Date().toISOString(),
      state: "committed",
      sizeBytes,
      contentChecksum,
      chunking: { strategy: "fixed", chunkSizeBytes: this.#chunkSizeBytes },
      chunks,
      metadata: { ...options.metadata, policyId: options.policyId }
    };
    const manifest = {
      ...manifestContent,
      manifestChecksum: checksumFor(Buffer.from(canonicalJson(manifestContent)))
    };
    validateManifest(manifest);
    const published = await this.#metadata.publishManifest(manifest, {
      expectedCurrentVersion: options.expectedCurrentVersion,
      policyId: options.policyId
    });
    return freezeClone({ ...published, policyId: options.policyId, manifest });
  }

  /**
   * Return a stream in original object order. Each returned chunk is hashed
   * before it reaches the caller; failed replicas are skipped automatically.
   */
  async getObject(objectId, { version } = {}) {
    assertObjectId(objectId);
    if (version !== undefined) assertVersion(version);
    const manifest = this.#metadata.getManifest(objectId, { version });
    const output = new PassThrough();
    const completion = this.#streamObject(manifest, output);
    completion.catch((error) => output.destroy(error));
    return Object.freeze({ objectId: manifest.objectId, version: manifest.version, sizeBytes: manifest.sizeBytes, manifest, stream: output, completion });
  }

  async #streamObject(manifest, output) {
    const policy = this.#metadata.getDurabilityPolicy(manifest.metadata.policyId ?? "standard");
    const contentHash = createHash("sha256");
    try {
      for (const chunk of manifest.chunks) {
        const bytes = await this.#readVerifiedChunk(chunk, policy.readQuorum);
        contentHash.update(bytes);
        if (!output.write(bytes)) await onceDrain(output);
      }
      const observedChecksum = contentHash.digest("hex");
      if (observedChecksum !== manifest.contentChecksum.value) {
        throw new ObjectApiError("OBJECT_CHECKSUM_MISMATCH", `Object ${manifest.objectId} content checksum failed`);
      }
      output.end();
    } catch (error) {
      output.destroy(error);
      throw error;
    }
  }

  async #readVerifiedChunk(chunk, requiredReadAcknowledgements) {
    const failures = [];
    const pending = [];
    for (const replica of chunk.replicas) {
      if (replica.state !== "healthy") continue;
      try {
        const node = this.#metadata.getNode(replica.nodeId);
        pending.push(readReplica(this.#transport, node, chunk)
          .then((bytes) => ({ ok: true, nodeId: replica.nodeId, bytes }))
          .catch((error) => ({ ok: false, nodeId: replica.nodeId, error })));
      } catch (error) {
        failures.push({ nodeId: replica.nodeId, code: error?.code ?? "NODE_NOT_FOUND" });
      }
    }

    const verified = [];
    while (pending.length > 0) {
      const outcome = await Promise.race(pending.map((promise, index) => promise.then((result) => ({ ...result, index }))));
      pending.splice(outcome.index, 1);
      if (outcome.ok) {
        verified.push(outcome);
        if (verified.length >= requiredReadAcknowledgements) return verified[0].bytes;
      } else {
        failures.push({ nodeId: outcome.nodeId, code: outcome.error?.code ?? "READ_FAILED" });
      }
      if (verified.length + pending.length < requiredReadAcknowledgements) break;
    }
    throw new ObjectApiError(
      "READ_QUORUM_UNAVAILABLE",
      `Chunk ${chunk.chunkId} did not receive ${requiredReadAcknowledgements} verified read responses`,
      { details: { chunkId: chunk.chunkId, requiredReadAcknowledgements, failures } }
    );
  }

  async #withObjectLock(objectId, operation) {
    const previous = this.#objectLocks.get(objectId) ?? Promise.resolve();
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const queued = previous.then(() => gate);
    this.#objectLocks.set(objectId, queued);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.#objectLocks.get(objectId) === queued) this.#objectLocks.delete(objectId);
    }
  }
}

async function writeQuorum(transport, plan, chunkId, bytes) {
  const pending = plan.writeTargets.map((target) =>
    transport.putChunk(target, chunkId, bytes)
      .then((ack) => ({ ok: true, ack, target }))
      .catch((error) => ({ ok: false, error, target }))
  );
  const acknowledgements = [];
  const failures = [];
  while (pending.length > 0) {
    const outcome = await Promise.race(pending.map((promise, index) => promise.then((result) => ({ ...result, index }))));
    pending.splice(outcome.index, 1);
    if (outcome.ok) {
      acknowledgements.push(outcome.ack);
      if (acknowledgements.length >= plan.requiredNewAcknowledgements) {
        return freezeClone(acknowledgements.map((ack) => ({ nodeId: ack.nodeId, state: "healthy", storedAt: ack.storedAt })));
      }
    } else {
      failures.push(serializeFailure(outcome));
    }
    if (acknowledgements.length + pending.length < plan.requiredNewAcknowledgements) break;
  }
  throw new ObjectApiError(
    "WRITE_QUORUM_NOT_ACKNOWLEDGED",
    `Chunk ${chunkId} received ${acknowledgements.length} new acknowledgements but requires ${plan.requiredNewAcknowledgements}`,
    { details: { chunkId, requiredNewAcknowledgements: plan.requiredNewAcknowledgements, failures } }
  );
}

async function readReplica(transport, node, chunk) {
  const result = await transport.getChunk(node, chunk.chunkId);
  if (result.sizeBytes !== chunk.sizeBytes) throw new ObjectApiError("CHUNK_LENGTH_MISMATCH", `Replica ${node.nodeId} returned an invalid chunk length`);
  return collectAndHash(result.stream, chunk.checksum.value);
}

async function collectAndHash(stream, expectedDigest) {
  const hash = createHash("sha256");
  const buffers = [];
  for await (const bytes of stream) {
    hash.update(bytes);
    buffers.push(bytes);
  }
  const actualDigest = hash.digest("hex");
  if (actualDigest !== expectedDigest) throw new ObjectApiError("CHUNK_CHECKSUM_MISMATCH", "Replica returned corrupted chunk bytes");
  return Buffer.concat(buffers);
}

function onceDrain(stream) {
  return new Promise((resolve, reject) => {
    stream.once("drain", resolve);
    stream.once("error", reject);
  });
}

function serializeFailure(outcome) {
  return { nodeId: outcome.target.nodeId, code: outcome.error?.code ?? "WRITE_FAILED" };
}

function fingerprint(value) {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

function assertObjectId(value) {
  if (typeof value !== "string" || !/^obj_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/.test(value)) throw new TypeError("objectId has an invalid format");
}

function assertVersion(value) {
  if (typeof value !== "string" || !/^ver_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/.test(value)) throw new TypeError("version has an invalid format");
}

function assertExpectedVersion(value) {
  if (value !== null) assertVersion(value);
}

function assertMetadata(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("metadata must be a JSON object");
  canonicalJson(value);
}

function assertIdempotencyKey(value) {
  if (typeof value !== "string" || value.length < 1 || value.length > 256) throw new TypeError("idempotencyKey must be a string of 1 to 256 characters");
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
