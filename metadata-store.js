import { randomUUID, timingSafeEqual } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname } from "node:path";

import { sha256 } from "../core/checksum.js";
import { canonicalJson, validateManifest } from "../core/manifest.js";
import { MetadataError } from "./errors.js";

const STORE_SCHEMA_VERSION = 1;
const NODE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
const POLICY_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
const NODE_STATES = new Set(["active", "draining", "removed"]);
const NODE_LIVENESS = new Set(["healthy", "unreachable"]);
const NODE_HEALTH = new Set(["healthy", "read-only", "degraded", "unavailable"]);
const DEFAULT_POLICY = Object.freeze({
  id: "standard",
  replicationFactor: 3,
  writeQuorum: 2,
  readQuorum: 2
});

/**
 * A single-writer, file-backed metadata store. Each committed change is
 * written to a checksummed temporary file and atomically renamed into place.
 * It is intentionally an interface boundary for a future replicated metadata
 * quorum; all callers use CAS rather than relying on process-local state.
 */
export class MetadataStore {
  #filePath;
  #heartbeatTimeoutMs;
  #clock;
  #data;
  #mutationLock = Promise.resolve();

  constructor({ filePath, heartbeatTimeoutMs = 30_000, clock = () => Date.now() }) {
    if (typeof filePath !== "string" || filePath.length === 0) throw new TypeError("filePath must be a non-empty path");
    if (!Number.isSafeInteger(heartbeatTimeoutMs) || heartbeatTimeoutMs <= 0) {
      throw new TypeError("heartbeatTimeoutMs must be a positive safe integer");
    }
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    this.#filePath = filePath;
    this.#heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.#clock = clock;
  }

  async initialize() {
    try {
      this.#data = readEnvelope(await readFile(this.#filePath, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      this.#data = emptyData();
      await this.#persist(this.#data);
    }
    validateStoreData(this.#data);
    return this;
  }

  snapshot() {
    this.#assertInitialized();
    return freezeClone({
      generation: this.#data.generation,
      policies: Object.values(this.#data.policies),
      nodes: Object.values(this.#data.nodes),
      objectCount: Object.keys(this.#data.objects).length
    });
  }

  getDurabilityPolicy(policyId = DEFAULT_POLICY.id) {
    this.#assertInitialized();
    const policy = this.#data.policies[policyId];
    if (!policy) throw new MetadataError("POLICY_NOT_FOUND", `Policy ${policyId} was not found`, { statusCode: 404 });
    return freezeClone(policy);
  }

  listDurabilityPolicies() {
    this.#assertInitialized();
    return freezeClone(Object.values(this.#data.policies).sort((a, b) => a.id.localeCompare(b.id)));
  }

  async upsertDurabilityPolicy(policy) {
    const normalized = normalizePolicy(policy);
    return this.#mutate((data) => {
      data.policies[normalized.id] = normalized;
      return normalized;
    });
  }

  async registerNode(node) {
    const normalized = normalizeNodeRegistration(node);
    return this.#mutate((data) => {
      const existing = data.nodes[normalized.nodeId];
      const now = this.#timestamp();
      if (existing?.state === "removed") {
        throw new MetadataError("NODE_REMOVED", `Node ${normalized.nodeId} was removed and cannot re-register`, { statusCode: 409 });
      }
      const next = {
        ...existing,
        ...normalized,
        state: existing?.state ?? "active",
        liveness: "healthy",
        registeredAt: existing?.registeredAt ?? now,
        lastHeartbeatAt: now,
        usedBytes: existing?.usedBytes ?? 0,
        reservedBytes: existing?.reservedBytes ?? 0,
        reportedHealth: existing?.reportedHealth ?? "healthy"
      };
      data.nodes[normalized.nodeId] = next;
      return next;
    });
  }

  getNode(nodeId) {
    this.#assertInitialized();
    assertNodeId(nodeId);
    const node = this.#data.nodes[nodeId];
    if (!node) throw new MetadataError("NODE_NOT_FOUND", `Node ${nodeId} was not found`, { statusCode: 404 });
    return freezeClone(node);
  }

  listNodes({ includeRemoved = false } = {}) {
    this.#assertInitialized();
    return freezeClone(
      Object.values(this.#data.nodes)
        .filter((node) => includeRemoved || node.state !== "removed")
        .sort((a, b) => a.nodeId.localeCompare(b.nodeId))
    );
  }

  async heartbeat(nodeId, report = {}) {
    assertNodeId(nodeId);
    const normalized = normalizeHeartbeat(report);
    return this.#mutate((data) => {
      const node = data.nodes[nodeId];
      if (!node) throw new MetadataError("NODE_NOT_FOUND", `Node ${nodeId} was not registered`, { statusCode: 404 });
      if (node.state === "removed") throw new MetadataError("NODE_REMOVED", `Node ${nodeId} was removed`, { statusCode: 409 });
      const next = { ...node, ...normalized, lastHeartbeatAt: this.#timestamp(), liveness: "healthy" };
      data.nodes[nodeId] = next;
      return next;
    });
  }

  async setNodeState(nodeId, state) {
    assertNodeId(nodeId);
    if (!NODE_STATES.has(state)) throw new TypeError("Node state must be active, draining, or removed");
    return this.#mutate((data) => {
      const node = data.nodes[nodeId];
      if (!node) throw new MetadataError("NODE_NOT_FOUND", `Node ${nodeId} was not found`, { statusCode: 404 });
      const next = { ...node, state };
      data.nodes[nodeId] = next;
      return next;
    });
  }

  /** Mark nodes unreachable when no heartbeat is received before the deadline. */
  async reconcileNodeLiveness() {
    return this.#mutate((data) => {
      const now = this.#clock();
      const changed = [];
      for (const [nodeId, node] of Object.entries(data.nodes)) {
        if (node.state === "removed" || node.liveness === "unreachable") continue;
        if (now - Date.parse(node.lastHeartbeatAt) > this.#heartbeatTimeoutMs) {
          const next = { ...node, liveness: "unreachable" };
          data.nodes[nodeId] = next;
          changed.push(next);
        }
      }
      return changed;
    });
  }

  /**
   * Publish an immutable committed manifest. expectedCurrentVersion must be
   * null for object creation or the currently observed version for an update.
   */
  async publishManifest(manifest, options) {
    this.#assertInitialized();
    if (!isRecord(options) || !Object.hasOwn(options, "expectedCurrentVersion")) {
      throw new TypeError("expectedCurrentVersion must be explicit (null for a new object)");
    }
    const { expectedCurrentVersion } = options;
    validateManifest(manifest);
    if (manifest.state !== "committed") {
      throw new MetadataError("MANIFEST_NOT_COMMITTED", "Only committed manifests may be published", { statusCode: 409 });
    }
    const immutableManifest = structuredClone(manifest);
    return this.#mutate((data) => {
      const current = data.objects[immutableManifest.objectId];
      const prior = current?.versions[immutableManifest.version];
      if (prior) {
        if (prior.manifest.manifestChecksum.value !== immutableManifest.manifestChecksum.value) {
          throw new MetadataError("VERSION_IMMUTABLE", `Version ${immutableManifest.version} is already published with different content`, { statusCode: 409 });
        }
        return { objectId: immutableManifest.objectId, version: immutableManifest.version, currentVersion: current.currentVersion, created: false };
      }
      const actualCurrentVersion = current?.currentVersion ?? null;
      if (actualCurrentVersion !== expectedCurrentVersion) {
        throw new MetadataError("VERSION_CONFLICT", "Object current version no longer matches the expected value", { statusCode: 409 });
      }
      const now = this.#timestamp();
      const next = {
        objectId: immutableManifest.objectId,
        currentVersion: immutableManifest.version,
        policyId: current?.policyId ?? DEFAULT_POLICY.id,
        createdAt: current?.createdAt ?? now,
        updatedAt: now,
        versions: {
          ...(current?.versions ?? {}),
          [immutableManifest.version]: { manifest: immutableManifest, publishedAt: now }
        }
      };
      data.objects[immutableManifest.objectId] = next;
      return { objectId: next.objectId, version: immutableManifest.version, currentVersion: next.currentVersion, created: true };
    });
  }

  getManifest(objectId, { version } = {}) {
    this.#assertInitialized();
    assertObjectId(objectId);
    const object = this.#data.objects[objectId];
    if (!object) throw new MetadataError("OBJECT_NOT_FOUND", `Object ${objectId} was not found`, { statusCode: 404 });
    const selectedVersion = version ?? object.currentVersion;
    const record = object.versions[selectedVersion];
    if (!record) throw new MetadataError("VERSION_NOT_FOUND", `Version ${selectedVersion} was not found`, { statusCode: 404 });
    return freezeClone(record.manifest);
  }

  getObject(objectId) {
    this.#assertInitialized();
    assertObjectId(objectId);
    const object = this.#data.objects[objectId];
    if (!object) throw new MetadataError("OBJECT_NOT_FOUND", `Object ${objectId} was not found`, { statusCode: 404 });
    return freezeClone({
      objectId: object.objectId,
      currentVersion: object.currentVersion,
      policyId: object.policyId,
      createdAt: object.createdAt,
      updatedAt: object.updatedAt,
      versions: Object.keys(object.versions).sort()
    });
  }

  async setObjectPolicy(objectId, policyId, options) {
    this.#assertInitialized();
    assertObjectId(objectId);
    assertPolicyId(policyId);
    if (!isRecord(options) || !Object.hasOwn(options, "expectedCurrentVersion")) {
      throw new TypeError("expectedCurrentVersion must be explicit");
    }
    const { expectedCurrentVersion } = options;
    return this.#mutate((data) => {
      const object = data.objects[objectId];
      if (!object) throw new MetadataError("OBJECT_NOT_FOUND", `Object ${objectId} was not found`, { statusCode: 404 });
      if (!data.policies[policyId]) throw new MetadataError("POLICY_NOT_FOUND", `Policy ${policyId} was not found`, { statusCode: 404 });
      if (object.currentVersion !== expectedCurrentVersion) {
        throw new MetadataError("VERSION_CONFLICT", "Object current version no longer matches the expected value", { statusCode: 409 });
      }
      const next = { ...object, policyId, updatedAt: this.#timestamp() };
      data.objects[objectId] = next;
      return next;
    });
  }

  async #mutate(mutator) {
    this.#assertInitialized();
    const previous = this.#mutationLock;
    let release;
    this.#mutationLock = new Promise((resolve) => { release = resolve; });
    await previous;
    try {
      const candidate = structuredClone(this.#data);
      const result = await mutator(candidate);
      candidate.generation += 1;
      validateStoreData(candidate);
      await this.#persist(candidate);
      this.#data = candidate;
      return freezeClone(result);
    } finally {
      release();
    }
  }

  #timestamp() {
    const value = this.#clock();
    if (!Number.isFinite(value)) throw new Error("clock must return a valid timestamp");
    return new Date(value).toISOString();
  }

  async #persist(data) {
    await writeEnvelopeAtomically(this.#filePath, data);
  }

  #assertInitialized() {
    if (!this.#data) throw new Error("MetadataStore.initialize() must be called first");
  }
}

function emptyData() {
  return {
    schemaVersion: STORE_SCHEMA_VERSION,
    generation: 0,
    policies: { [DEFAULT_POLICY.id]: structuredClone(DEFAULT_POLICY) },
    nodes: {},
    objects: {}
  };
}

function normalizePolicy(policy) {
  if (!isRecord(policy)) throw new TypeError("Policy must be an object");
  assertExactKeys("Policy", policy, ["id", "replicationFactor", "writeQuorum", "readQuorum"]);
  assertPolicyId(policy.id);
  for (const field of ["replicationFactor", "writeQuorum", "readQuorum"]) {
    if (!Number.isSafeInteger(policy[field]) || policy[field] <= 0) throw new TypeError(`${field} must be a positive safe integer`);
  }
  if (policy.writeQuorum > policy.replicationFactor || policy.readQuorum > policy.replicationFactor) {
    throw new TypeError("Read and write quorums cannot exceed replicationFactor");
  }
  if (policy.readQuorum + policy.writeQuorum <= policy.replicationFactor) {
    throw new TypeError("readQuorum + writeQuorum must exceed replicationFactor to intersect");
  }
  return structuredClone(policy);
}

function normalizeNodeRegistration(node) {
  if (!isRecord(node)) throw new TypeError("Node registration must be an object");
  assertOnlyKeys("Node registration", node, ["nodeId", "endpoint", "capacityBytes", "zone", "rack"]);
  const normalized = { zone: null, rack: null, ...node };
  assertNodeId(normalized.nodeId);
  if (typeof normalized.endpoint !== "string") throw new TypeError("endpoint must be a string");
  let endpoint;
  try { endpoint = new URL(normalized.endpoint); } catch { throw new TypeError("endpoint must be an absolute HTTP(S) URL"); }
  if (!["http:", "https:"].includes(endpoint.protocol)) throw new TypeError("endpoint must use HTTP or HTTPS");
  assertPositiveInteger("capacityBytes", normalized.capacityBytes);
  assertOptionalLocation("zone", normalized.zone);
  assertOptionalLocation("rack", normalized.rack);
  return structuredClone(normalized);
}

function normalizeHeartbeat(report) {
  if (!isRecord(report)) throw new TypeError("Heartbeat report must be an object");
  assertExactKeys("Heartbeat report", report, ["usedBytes", "reservedBytes", "capacityBytes", "reportedHealth"]);
  assertNonNegativeInteger("usedBytes", report.usedBytes);
  assertNonNegativeInteger("reservedBytes", report.reservedBytes);
  assertPositiveInteger("capacityBytes", report.capacityBytes);
  if (report.usedBytes + report.reservedBytes > report.capacityBytes) throw new TypeError("Heartbeat usage exceeds capacity");
  if (!NODE_HEALTH.has(report.reportedHealth)) throw new TypeError("reportedHealth is invalid");
  return structuredClone(report);
}

function validateStoreData(data) {
  if (!isRecord(data)) throw new MetadataError("INVALID_STORE", "Metadata data must be an object");
  assertExactKeys("Metadata data", data, ["schemaVersion", "generation", "policies", "nodes", "objects"]);
  if (data.schemaVersion !== STORE_SCHEMA_VERSION) throw new MetadataError("INVALID_STORE", "Unsupported metadata schema version");
  assertNonNegativeInteger("generation", data.generation);
  if (!isRecord(data.policies) || !isRecord(data.nodes) || !isRecord(data.objects)) throw new MetadataError("INVALID_STORE", "Metadata maps are malformed");
  for (const [id, policy] of Object.entries(data.policies)) {
    if (id !== policy?.id) throw new MetadataError("INVALID_STORE", "Policy map key does not match policy ID");
    normalizePolicy(policy);
  }
  if (!data.policies[DEFAULT_POLICY.id]) throw new MetadataError("INVALID_STORE", "Standard durability policy is missing");
  for (const [id, node] of Object.entries(data.nodes)) validateStoredNode(id, node);
  for (const [id, object] of Object.entries(data.objects)) validateStoredObject(id, object, data.policies);
}

function validateStoredNode(id, node) {
  if (!isRecord(node)) throw new MetadataError("INVALID_STORE", "Node record is malformed");
  assertNodeId(id);
  if (node.nodeId !== id || !NODE_STATES.has(node.state) || !NODE_LIVENESS.has(node.liveness)) {
    throw new MetadataError("INVALID_STORE", "Node identity or state is invalid");
  }
  normalizeNodeRegistration({ nodeId: node.nodeId, endpoint: node.endpoint, capacityBytes: node.capacityBytes, zone: node.zone, rack: node.rack });
  normalizeHeartbeat({ usedBytes: node.usedBytes, reservedBytes: node.reservedBytes, capacityBytes: node.capacityBytes, reportedHealth: node.reportedHealth });
  assertTimestamp("registeredAt", node.registeredAt);
  assertTimestamp("lastHeartbeatAt", node.lastHeartbeatAt);
}

function validateStoredObject(id, object, policies) {
  if (!isRecord(object) || object.objectId !== id || !isRecord(object.versions)) throw new MetadataError("INVALID_STORE", "Object record is malformed");
  assertObjectId(id);
  if (typeof object.currentVersion !== "string" || !policies[object.policyId]) throw new MetadataError("INVALID_STORE", "Object version or policy is invalid");
  assertTimestamp("createdAt", object.createdAt);
  assertTimestamp("updatedAt", object.updatedAt);
  if (!object.versions[object.currentVersion]) throw new MetadataError("INVALID_STORE", "Object current version is missing");
  for (const [version, record] of Object.entries(object.versions)) {
    if (!isRecord(record) || !record.manifest || typeof record.publishedAt !== "string") throw new MetadataError("INVALID_STORE", "Object version record is malformed");
    validateManifest(record.manifest);
    if (record.manifest.state !== "committed" || record.manifest.objectId !== id || record.manifest.version !== version) {
      throw new MetadataError("INVALID_STORE", "Stored manifest is not a committed matching version");
    }
    assertTimestamp("publishedAt", record.publishedAt);
  }
}

function readEnvelope(raw) {
  let envelope;
  try { envelope = JSON.parse(raw); } catch { throw new MetadataError("METADATA_CORRUPT", "Metadata file is not valid JSON"); }
  if (!isRecord(envelope)) throw new MetadataError("METADATA_CORRUPT", "Metadata envelope is malformed");
  assertExactKeys("Metadata envelope", envelope, ["schemaVersion", "checksum", "data"]);
  if (envelope.schemaVersion !== STORE_SCHEMA_VERSION || typeof envelope.checksum !== "string" || !/^[a-f0-9]{64}$/.test(envelope.checksum)) {
    throw new MetadataError("METADATA_CORRUPT", "Metadata envelope is invalid");
  }
  const actual = sha256(Buffer.from(canonicalJson(envelope.data)));
  if (!safeEquals(actual, envelope.checksum)) throw new MetadataError("METADATA_CORRUPT", "Metadata checksum does not match stored content");
  return envelope.data;
}

async function writeEnvelopeAtomically(filePath, data) {
  await mkdir(dirname(filePath), { recursive: true });
  const envelope = { schemaVersion: STORE_SCHEMA_VERSION, checksum: sha256(Buffer.from(canonicalJson(data))), data };
  const temporaryPath = `${filePath}.tmp-${randomUUID()}`;
  let handle;
  try {
    handle = await open(temporaryPath, "wx");
    await handle.writeFile(JSON.stringify(envelope));
    await handle.sync();
    await handle.close();
    handle = undefined;
    await rename(temporaryPath, filePath);
  } finally {
    await handle?.close();
    try { await unlink(temporaryPath); } catch (error) { if (error?.code !== "ENOENT") throw error; }
  }
}

function assertNodeId(value) {
  if (typeof value !== "string" || !NODE_ID_PATTERN.test(value)) throw new TypeError("nodeId has an invalid format");
}

function assertObjectId(value) {
  if (typeof value !== "string" || !/^obj_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/.test(value)) throw new TypeError("objectId has an invalid format");
}

function assertPolicyId(value) {
  if (typeof value !== "string" || !POLICY_ID_PATTERN.test(value)) throw new TypeError("policy ID has an invalid format");
}

function assertOptionalLocation(name, value) {
  if (value !== null && (typeof value !== "string" || !NODE_ID_PATTERN.test(value))) throw new TypeError(`${name} must be null or a valid identifier`);
}

function assertPositiveInteger(name, value) {
  if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`${name} must be a positive safe integer`);
}

function assertNonNegativeInteger(name, value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a non-negative safe integer`);
}

function assertTimestamp(name, value) {
  if (typeof value !== "string" || Number.isNaN(Date.parse(value))) throw new MetadataError("INVALID_STORE", `${name} is not a valid timestamp`);
}

function assertExactKeys(name, value, keys) {
  if (!isRecord(value)) throw new TypeError(`${name} must be an object`);
  for (const key of Object.keys(value)) if (!keys.includes(key)) throw new TypeError(`${name}.${key} is not supported`);
  for (const key of keys) if (!Object.hasOwn(value, key)) throw new TypeError(`${name}.${key} is required`);
}

function assertOnlyKeys(name, value, keys) {
  if (!isRecord(value)) throw new TypeError(`${name} must be an object`);
  for (const key of Object.keys(value)) if (!keys.includes(key)) throw new TypeError(`${name}.${key} is not supported`);
}

function isRecord(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function safeEquals(left, right) {
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
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
