/**
 * Coalescing in-memory repair queue. Each key has at most one
 * outstanding job; a newer observation may raise its priority and merge the
 * reason list without creating duplicate copy work.
 */
export class RepairQueue {
  #jobs = new Map();
  #sequence = 0;

  enqueue(job) {
    const normalized = normalizeJob(job);
    const key = jobKey(normalized);
    const existing = this.#jobs.get(key);
    if (existing) {
      const merged = {
        ...existing,
        priority: Math.max(existing.priority, normalized.priority),
        reasons: [...new Set([...existing.reasons, ...normalized.reasons])].sort(),
        observedAt: normalized.observedAt,
        attempts: existing.attempts,
        sequence: existing.sequence
      };
      this.#jobs.set(key, merged);
      return freezeClone({ ...merged, enqueued: false });
    }
    const record = { ...normalized, attempts: 0, sequence: this.#sequence++ };
    this.#jobs.set(key, record);
    return freezeClone({ ...record, enqueued: true });
  }

  /** Claim the highest-priority queued repair. Claimed work is removed. */
  claim() {
    const jobs = [...this.#jobs.values()];
    if (jobs.length === 0) return null;
    jobs.sort((left, right) => right.priority - left.priority || left.sequence - right.sequence);
    const job = jobs[0];
    this.#jobs.delete(jobKey(job));
    return freezeClone({ ...job, attempts: job.attempts + 1 });
  }

  list() {
    return freezeClone([...this.#jobs.values()].sort((left, right) => right.priority - left.priority || left.sequence - right.sequence));
  }

  get size() {
    return this.#jobs.size;
  }
}

function normalizeJob(job) {
  if (!isRecord(job)) throw new TypeError("Repair job must be an object");
  const allowed = ["objectId", "version", "chunk", "sourceNodeId", "targetNodeId", "reasons", "priority", "observedAt"];
  for (const key of Object.keys(job)) if (!allowed.includes(key)) throw new TypeError(`Repair job field ${key} is not supported`);
  const { objectId, version, chunk, sourceNodeId, targetNodeId, reasons, priority = 0, observedAt = new Date().toISOString() } = job;
  assertId("objectId", objectId, /^obj_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/);
  assertId("version", version, /^ver_[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/);
  if (!isRecord(chunk) || typeof chunk.chunkId !== "string" || !/^chk_[a-f0-9]{64}$/.test(chunk.chunkId) || !Number.isSafeInteger(chunk.index)) {
    throw new TypeError("Repair job chunk must be a valid chunk descriptor");
  }
  assertId("sourceNodeId", sourceNodeId, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/);
  assertId("targetNodeId", targetNodeId, /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/);
  if (sourceNodeId === targetNodeId) throw new TypeError("Repair source and target must differ");
  if (!Array.isArray(reasons) || reasons.length === 0 || reasons.some((reason) => typeof reason !== "string" || reason.length === 0)) {
    throw new TypeError("Repair job reasons must be a non-empty string array");
  }
  if (!Number.isSafeInteger(priority) || priority < 0 || priority > 100) throw new TypeError("Repair job priority must be an integer from 0 to 100");
  if (typeof observedAt !== "string" || Number.isNaN(Date.parse(observedAt))) throw new TypeError("observedAt must be a timestamp");
  return { objectId, version, chunk: structuredClone(chunk), sourceNodeId, targetNodeId, reasons: [...new Set(reasons)].sort(), priority, observedAt };
}

function jobKey(job) {
  return `${job.objectId}\u0000${job.version}\u0000${job.chunk.chunkId}\u0000${job.targetNodeId}`;
}

function assertId(name, value, pattern) {
  if (typeof value !== "string" || !pattern.test(value)) throw new TypeError(`${name} has an invalid format`);
}

function isRecord(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
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
