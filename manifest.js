import { randomUUID } from "node:crypto";

import { CHECKSUM_ALGORITHM, checksumFor, createContentHasher, sha256 } from "./checksum.js";
import { fixedSizeChunks } from "./chunker.js";

export const MANIFEST_SCHEMA_VERSION = 1;
export const DEFAULT_CHUNK_SIZE_BYTES = 8 * 1024 * 1024;

const OBJECT_ID_PATTERN = /^obj_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
const VERSION_PATTERN = /^ver_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
const NODE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
const CHECKSUM_PATTERN = /^[a-f0-9]{64}$/;
const REPLICA_STATES = new Set(["healthy", "stale", "repairing", "unavailable"]);

/** Create a valid, externally safe object identifier. */
export function newObjectId() {
  return `obj_${randomUUID()}`;
}

/** Create a version identifier. Object versions are immutable once committed. */
export function newVersionId() {
  return `ver_${randomUUID()}`;
}

/**
 * Stream an object into a compact, immutable manifest.
 *
 * `placementForChunk` receives the chunk descriptor and returns the replica
 * records currently known for that chunk. It may be async, enabling the next
 * storage-node layer to write the bytes before returning their locations.
 */
export async function buildManifestFromStream(input, options = {}) {
  const {
    objectId = newObjectId(),
    version = newVersionId(),
    chunkSizeBytes = DEFAULT_CHUNK_SIZE_BYTES,
    createdAt = new Date().toISOString(),
    state = "draft",
    metadata = {},
    placementForChunk = () => []
  } = options;

  assertIdentifier("objectId", objectId, OBJECT_ID_PATTERN);
  assertIdentifier("version", version, VERSION_PATTERN);
  assertTimestamp(createdAt);
  assertState(state);
  assertMetadata(metadata);
  if (typeof placementForChunk !== "function") throw new TypeError("placementForChunk must be a function");

  const contentHasher = createContentHasher();
  const chunks = [];
  let sizeBytes = 0;

  for await (const bytes of fixedSizeChunks(input, chunkSizeBytes)) {
    contentHasher.update(bytes);
    sizeBytes += bytes.length;
    const checksum = checksumFor(bytes);
    const descriptor = {
      index: chunks.length,
      chunkId: `chk_${checksum.value}`,
      sizeBytes: bytes.length,
      checksum,
      replicas: []
    };
    const replicas = await placementForChunk(deepFreeze(structuredClone(descriptor)), bytes);
    descriptor.replicas = normalizeReplicas(replicas);
    chunks.push(descriptor);
  }

  const withoutManifestChecksum = {
    schemaVersion: MANIFEST_SCHEMA_VERSION,
    objectId,
    version,
    createdAt,
    state,
    sizeBytes,
    contentChecksum: contentHasher.digest(),
    chunking: { strategy: "fixed", chunkSizeBytes },
    chunks,
    metadata: structuredClone(metadata)
  };

  const manifest = {
    ...withoutManifestChecksum,
    manifestChecksum: checksumFor(Buffer.from(canonicalJson(withoutManifestChecksum)))
  };

  validateManifest(manifest);
  return deepFreeze(manifest);
}

/**
 * Promote a placement-complete draft manifest. This creates a new immutable
 * manifest checksum; it does not mutate the source object.
 */
export function commitManifest(manifest) {
  validateManifest(manifest);
  const candidate = structuredClone(manifest);
  candidate.state = "committed";
  candidate.manifestChecksum = checksumFor(Buffer.from(canonicalJson(manifestContent(candidate))));
  validateManifest(candidate);
  return deepFreeze(candidate);
}

/** Validate untrusted manifest data and throw a descriptive Error if invalid. */
export function validateManifest(manifest) {
  if (!isRecord(manifest)) fail("manifest must be an object");
  assertOnlyKeys("manifest", manifest, [
    "schemaVersion", "objectId", "version", "createdAt", "state", "sizeBytes",
    "contentChecksum", "chunking", "chunks", "metadata", "manifestChecksum"
  ]);
  if (manifest.schemaVersion !== MANIFEST_SCHEMA_VERSION) fail("unsupported schemaVersion");
  assertIdentifier("objectId", manifest.objectId, OBJECT_ID_PATTERN);
  assertIdentifier("version", manifest.version, VERSION_PATTERN);
  assertTimestamp(manifest.createdAt);
  assertState(manifest.state);
  if (!Number.isSafeInteger(manifest.sizeBytes) || manifest.sizeBytes < 0) fail("sizeBytes must be a non-negative safe integer");
  assertChecksum("contentChecksum", manifest.contentChecksum);
  validateChunking(manifest.chunking);
  if (!Array.isArray(manifest.chunks)) fail("chunks must be an array");
  assertMetadata(manifest.metadata);

  let calculatedSize = 0;
  for (let index = 0; index < manifest.chunks.length; index += 1) {
    const chunk = manifest.chunks[index];
    if (!isRecord(chunk)) fail(`chunks[${index}] must be an object`);
    assertOnlyKeys(`chunks[${index}]`, chunk, ["index", "chunkId", "sizeBytes", "checksum", "replicas"]);
    if (chunk.index !== index) fail(`chunks[${index}] has a non-contiguous index`);
    if (!Number.isSafeInteger(chunk.sizeBytes) || chunk.sizeBytes <= 0) fail(`chunks[${index}].sizeBytes must be positive`);
    assertChecksum(`chunks[${index}].checksum`, chunk.checksum);
    if (chunk.chunkId !== `chk_${chunk.checksum.value}`) fail(`chunks[${index}].chunkId does not match checksum`);
    if (!Array.isArray(chunk.replicas)) fail(`chunks[${index}].replicas must be an array`);
    if (manifest.state === "committed" && chunk.replicas.length === 0) {
      fail(`committed chunks[${index}] must have at least one replica`);
    }
    validateReplicas(chunk.replicas, index);
    if (index < manifest.chunks.length - 1 && chunk.sizeBytes !== manifest.chunking.chunkSizeBytes) {
      fail(`chunks[${index}] must match fixed chunkSizeBytes`);
    }
    if (chunk.sizeBytes > manifest.chunking.chunkSizeBytes) fail(`chunks[${index}] exceeds fixed chunkSizeBytes`);
    calculatedSize += chunk.sizeBytes;
  }
  if (calculatedSize !== manifest.sizeBytes) fail("sizeBytes does not match the sum of chunks");
  if (manifest.sizeBytes === 0 && manifest.chunks.length !== 0) fail("empty objects cannot have chunks");

  assertChecksum("manifestChecksum", manifest.manifestChecksum);
  const expectedManifestChecksum = sha256(Buffer.from(canonicalJson(manifestContent(manifest))));
  if (manifest.manifestChecksum.value !== expectedManifestChecksum) fail("manifestChecksum does not match manifest content");
  return true;
}

/** Stable JSON encoding used for manifest integrity verification. */
export function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Canonical JSON does not support non-finite numbers");
    return JSON.stringify(value);
  }
  if (!isRecord(value)) throw new TypeError("Canonical JSON supports JSON-compatible values only");
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
}

function manifestContent(manifest) {
  const { manifestChecksum: _manifestChecksum, ...content } = manifest;
  return content;
}

function normalizeReplicas(replicas) {
  if (replicas === undefined) return [];
  if (!Array.isArray(replicas)) throw new TypeError("placementForChunk must return an array of replica locations");
  return replicas.map((replica) => structuredClone(replica));
}

function validateChunking(chunking) {
  if (!isRecord(chunking) || chunking.strategy !== "fixed") fail("chunking.strategy must be fixed");
  assertOnlyKeys("chunking", chunking, ["strategy", "chunkSizeBytes"]);
  if (!Number.isSafeInteger(chunking.chunkSizeBytes) || chunking.chunkSizeBytes <= 0) {
    fail("chunking.chunkSizeBytes must be a positive safe integer");
  }
}

function validateReplicas(replicas, chunkIndex) {
  const nodeIds = new Set();
  for (let replicaIndex = 0; replicaIndex < replicas.length; replicaIndex += 1) {
    const replica = replicas[replicaIndex];
    const path = `chunks[${chunkIndex}].replicas[${replicaIndex}]`;
    if (!isRecord(replica)) fail(`${path} must be an object`);
    assertIdentifier(`${path}.nodeId`, replica.nodeId, NODE_ID_PATTERN);
    if (nodeIds.has(replica.nodeId)) fail(`${path}.nodeId is duplicated for this chunk`);
    nodeIds.add(replica.nodeId);
    if (!REPLICA_STATES.has(replica.state)) fail(`${path}.state is invalid`);
    if (replica.storedAt !== undefined) assertTimestamp(replica.storedAt);
    for (const key of Object.keys(replica)) {
      if (!["nodeId", "state", "storedAt"].includes(key)) fail(`${path}.${key} is not supported`);
    }
  }
}

function assertChecksum(path, checksum) {
  if (!isRecord(checksum) || checksum.algorithm !== CHECKSUM_ALGORITHM || typeof checksum.value !== "string" || !CHECKSUM_PATTERN.test(checksum.value)) {
    fail(`${path} must be a sha256 checksum`);
  }
  assertOnlyKeys(path, checksum, ["algorithm", "value"]);
}

function assertIdentifier(name, value, pattern) {
  if (typeof value !== "string" || !pattern.test(value)) fail(`${name} has an invalid format`);
}

function assertTimestamp(value) {
  const isoInstant = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;
  if (typeof value !== "string" || !isoInstant.test(value) || Number.isNaN(Date.parse(value))) {
    fail("createdAt/storedAt must be an ISO-8601 timestamp");
  }
}

function assertState(state) {
  if (!["draft", "committed"].includes(state)) fail("state must be draft or committed");
}

function assertMetadata(metadata) {
  if (!isRecord(metadata)) fail("metadata must be a JSON object");
  try {
    canonicalJson(metadata);
  } catch {
    fail("metadata must be JSON-compatible");
  }
}

function isRecord(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertOnlyKeys(path, value, keys) {
  for (const key of Object.keys(value)) {
    if (!keys.includes(key)) fail(`${path}.${key} is not supported`);
  }
}

function fail(message) {
  throw new Error(`Invalid manifest: ${message}`);
}

function deepFreeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}
