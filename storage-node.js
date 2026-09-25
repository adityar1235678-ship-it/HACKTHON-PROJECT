import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readdir, rename, stat, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

import { CHECKSUM_ALGORITHM } from "../core/checksum.js";
import { StorageError } from "./errors.js";

const CHUNK_ID_PATTERN = /^chk_([a-f0-9]{64})$/;
const DEFAULT_CAPACITY_BYTES = 100 * 1024 * 1024 * 1024;
const DEFAULT_FAULTS = Object.freeze({
  online: true,
  writable: true,
  partitioned: false,
  latencyMs: 0,
  corruptReads: false
});

/** Disk-backed, content-addressed storage node for chunks. */
export class StorageNode {
  #chunksDir;
  #nodeId;
  #capacityBytes;
  #usedBytes = 0;
  #reservedBytes = 0;
  #faults = { ...DEFAULT_FAULTS };
  #chunkLocks = new Map();
  #accountingLock = Promise.resolve();
  #corruptChunks = new Set();

  constructor({ nodeId, rootDir, capacityBytes = DEFAULT_CAPACITY_BYTES }) {
    assertNodeId(nodeId);
    if (typeof rootDir !== "string" || rootDir.length === 0) throw new TypeError("rootDir must be a non-empty path");
    if (!Number.isSafeInteger(capacityBytes) || capacityBytes <= 0) {
      throw new TypeError("capacityBytes must be a positive safe integer");
    }
    this.#nodeId = nodeId;
    this.#chunksDir = join(rootDir, "chunks");
    this.#capacityBytes = capacityBytes;
  }

  get nodeId() {
    return this.#nodeId;
  }

  /** Initialize directories and recover capacity usage after a restart. */
  async initialize() {
    await mkdir(this.#chunksDir, { recursive: true });
    this.#usedBytes = await this.#scanUsage();
    if (this.#usedBytes > this.#capacityBytes) {
      throw new StorageError("CAPACITY_EXCEEDED", "Existing chunks exceed configured node capacity", { statusCode: 507 });
    }
    return this;
  }

  health() {
    const reachable = this.#faults.online && !this.#faults.partitioned;
    return Object.freeze({
      nodeId: this.#nodeId,
      status: !reachable ? "unavailable" : !this.#faults.writable ? "read-only" : this.#corruptChunks.size > 0 ? "degraded" : "healthy",
      reachable,
      writable: reachable && this.#faults.writable,
      faults: { ...this.#faults },
      corruptChunkCount: this.#corruptChunks.size
    });
  }

  stats() {
    const health = this.health();
    return Object.freeze({
      ...health,
      capacityBytes: this.#capacityBytes,
      usedBytes: this.#usedBytes,
      reservedBytes: this.#reservedBytes,
      availableBytes: this.#capacityBytes - this.#usedBytes - this.#reservedBytes
    });
  }

  setFaults(patch) {
    if (!isPlainRecord(patch)) throw new TypeError("Fault patch must be an object");
    const allowed = new Set(Object.keys(DEFAULT_FAULTS));
    for (const key of Object.keys(patch)) {
      if (!allowed.has(key)) throw new TypeError(`Unsupported fault control: ${key}`);
    }
    const candidate = { ...this.#faults, ...patch };
    for (const key of ["online", "writable", "partitioned", "corruptReads"]) {
      if (typeof candidate[key] !== "boolean") throw new TypeError(`${key} must be a boolean`);
    }
    if (!Number.isSafeInteger(candidate.latencyMs) || candidate.latencyMs < 0 || candidate.latencyMs > 60_000) {
      throw new TypeError("latencyMs must be a safe integer between 0 and 60000");
    }
    this.#faults = candidate;
    return this.health();
  }

  /**
   * Atomically store a chunk. expectedSizeBytes must be known before writing
   * so the node can reserve space before accepting a streaming payload.
   */
  async putChunk(chunkId, input, { expectedSizeBytes } = {}) {
    this.#assertWriteAllowed();
    const digest = parseChunkId(chunkId);
    assertPositiveSize(expectedSizeBytes);
    await this.#maybeDelay();

    return this.#withChunkLock(chunkId, async () => {
      this.#assertWriteAllowed();
      const destination = this.#chunkPath(chunkId);
      const existing = await fileSizeOrNull(destination);
      if (existing !== null) {
        if (existing !== expectedSizeBytes) {
          throw new StorageError("CHUNK_CONFLICT", `Existing ${chunkId} has an unexpected length`, { statusCode: 409 });
        }
        await this.verifyChunk(chunkId);
        return Object.freeze({ chunkId, sizeBytes: existing, checksum: checksum(digest), created: false });
      }

      await this.#reserve(expectedSizeBytes);
      const tempPath = `${destination}.part-${randomUUID()}`;
      let published = false;
      try {
        await mkdir(dirname(destination), { recursive: true });
        const observed = await writeAndHash(input, tempPath);
        if (observed.sizeBytes !== expectedSizeBytes) {
          throw new StorageError("LENGTH_MISMATCH", `Expected ${expectedSizeBytes} bytes but received ${observed.sizeBytes}`, { statusCode: 400 });
        }
        if (!safeDigestEquals(observed.digest, digest)) {
          throw new StorageError("CHECKSUM_MISMATCH", `Chunk bytes do not match ${chunkId}`, { statusCode: 422 });
        }
        await rename(tempPath, destination);
        published = true;
        await this.#commitReservation(expectedSizeBytes);
        return Object.freeze({ chunkId, sizeBytes: expectedSizeBytes, checksum: checksum(digest), created: true });
      } finally {
        if (!published) {
          await unlinkIfPresent(tempPath);
          await this.#releaseReservation(expectedSizeBytes);
        }
      }
    });
  }

  /** Verify disk bytes before returning a stream to a caller. */
  async getChunk(chunkId) {
    this.#assertReadAllowed();
    const digest = parseChunkId(chunkId);
    await this.#maybeDelay();
    const filePath = this.#chunkPath(chunkId);
    const sizeBytes = await fileSizeOrNull(filePath);
    if (sizeBytes === null) throw new StorageError("CHUNK_NOT_FOUND", `Chunk ${chunkId} was not found`, { statusCode: 404 });
    await this.verifyChunk(chunkId);

    let stream = createReadStream(filePath);
    if (this.#faults.corruptReads && sizeBytes > 0) stream = stream.pipe(corruptFirstByte());
    return Object.freeze({ chunkId, sizeBytes, checksum: checksum(digest), stream });
  }

  async headChunk(chunkId) {
    this.#assertReadAllowed();
    const digest = parseChunkId(chunkId);
    await this.#maybeDelay();
    const sizeBytes = await fileSizeOrNull(this.#chunkPath(chunkId));
    if (sizeBytes === null) throw new StorageError("CHUNK_NOT_FOUND", `Chunk ${chunkId} was not found`, { statusCode: 404 });
    return Object.freeze({ chunkId, sizeBytes, checksum: checksum(digest) });
  }

  async deleteChunk(chunkId) {
    this.#assertWriteAllowed();
    parseChunkId(chunkId);
    await this.#maybeDelay();
    return this.#withChunkLock(chunkId, async () => {
      this.#assertWriteAllowed();
      const filePath = this.#chunkPath(chunkId);
      const sizeBytes = await fileSizeOrNull(filePath);
      if (sizeBytes === null) return Object.freeze({ chunkId, deleted: false });
      await unlink(filePath);
      await this.#withAccountingLock(() => {
        this.#usedBytes -= sizeBytes;
        this.#corruptChunks.delete(chunkId);
      });
      return Object.freeze({ chunkId, deleted: true, sizeBytes });
    });
  }

  /** Scrub a chunk in place and throw CORRUPT_CHUNK when verification fails. */
  async verifyChunk(chunkId) {
    this.#assertReadAllowed();
    const expectedDigest = parseChunkId(chunkId);
    const filePath = this.#chunkPath(chunkId);
    const expectedSize = await fileSizeOrNull(filePath);
    if (expectedSize === null) throw new StorageError("CHUNK_NOT_FOUND", `Chunk ${chunkId} was not found`, { statusCode: 404 });

    const hash = createHash(CHECKSUM_ALGORITHM);
    let observedSize = 0;
    try {
      for await (const bytes of createReadStream(filePath)) {
        observedSize += bytes.length;
        hash.update(bytes);
      }
      const observedDigest = hash.digest("hex");
      if (observedSize !== expectedSize || !safeDigestEquals(observedDigest, expectedDigest)) {
        throw new StorageError("CORRUPT_CHUNK", `Stored bytes for ${chunkId} failed integrity verification`, { statusCode: 409 });
      }
      this.#corruptChunks.delete(chunkId);
      return Object.freeze({ chunkId, sizeBytes: observedSize, checksum: checksum(observedDigest), verifiedAt: new Date().toISOString() });
    } catch (error) {
      if (error instanceof StorageError && error.code === "CORRUPT_CHUNK") this.#corruptChunks.add(chunkId);
      throw error;
    }
  }

  async listChunkIds() {
    this.#assertReadAllowed();
    await this.#maybeDelay();
    const ids = [];
    const buckets = await readdir(this.#chunksDir, { withFileTypes: true });
    for (const bucket of buckets) {
      if (!bucket.isDirectory() || !/^[a-f0-9]{2}$/.test(bucket.name)) continue;
      const files = await readdir(join(this.#chunksDir, bucket.name), { withFileTypes: true });
      for (const file of files) if (file.isFile() && CHUNK_ID_PATTERN.test(file.name)) ids.push(file.name);
    }
    return Object.freeze(ids.sort());
  }

  #chunkPath(chunkId) {
    const digest = parseChunkId(chunkId);
    return join(this.#chunksDir, digest.slice(0, 2), chunkId);
  }

  #assertReadAllowed() {
    if (!this.#faults.online || this.#faults.partitioned) {
      throw new StorageError("NODE_UNAVAILABLE", `Node ${this.#nodeId} is unreachable`, { statusCode: 503 });
    }
  }

  #assertWriteAllowed() {
    this.#assertReadAllowed();
    if (!this.#faults.writable) throw new StorageError("NODE_READ_ONLY", `Node ${this.#nodeId} is read-only`, { statusCode: 503 });
  }

  async #maybeDelay() {
    if (this.#faults.latencyMs > 0) await new Promise((resolve) => setTimeout(resolve, this.#faults.latencyMs));
  }

  async #reserve(sizeBytes) {
    await this.#withAccountingLock(() => {
      if (this.#usedBytes + this.#reservedBytes + sizeBytes > this.#capacityBytes) {
        throw new StorageError("CAPACITY_EXCEEDED", `Node ${this.#nodeId} lacks ${sizeBytes} free bytes`, { statusCode: 507 });
      }
      this.#reservedBytes += sizeBytes;
    });
  }

  async #commitReservation(sizeBytes) {
    await this.#withAccountingLock(() => {
      this.#reservedBytes -= sizeBytes;
      this.#usedBytes += sizeBytes;
    });
  }

  async #releaseReservation(sizeBytes) {
    await this.#withAccountingLock(() => {
      this.#reservedBytes -= sizeBytes;
    });
  }

  async #withAccountingLock(operation) {
    const previous = this.#accountingLock;
    let release;
    this.#accountingLock = new Promise((resolve) => { release = resolve; });
    await previous;
    try {
      return operation();
    } finally {
      release();
    }
  }

  async #withChunkLock(chunkId, operation) {
    const previous = this.#chunkLocks.get(chunkId) ?? Promise.resolve();
    let release;
    const current = new Promise((resolve) => { release = resolve; });
    const queued = previous.then(() => current);
    this.#chunkLocks.set(chunkId, queued);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.#chunkLocks.get(chunkId) === queued) this.#chunkLocks.delete(chunkId);
    }
  }

  async #scanUsage() {
    let total = 0;
    const buckets = await readdir(this.#chunksDir, { withFileTypes: true });
    for (const bucket of buckets) {
      if (!bucket.isDirectory() || !/^[a-f0-9]{2}$/.test(bucket.name)) continue;
      const files = await readdir(join(this.#chunksDir, bucket.name), { withFileTypes: true });
      for (const file of files) {
        if (file.isFile() && CHUNK_ID_PATTERN.test(file.name)) total += (await stat(join(this.#chunksDir, bucket.name, file.name))).size;
      }
    }
    return total;
  }
}

async function writeAndHash(input, tempPath) {
  const source = Buffer.isBuffer(input) || input instanceof Uint8Array ? [input] : input;
  if (source === null || source === undefined || typeof source[Symbol.asyncIterator] !== "function" && typeof source[Symbol.iterator] !== "function") {
    throw new TypeError("Chunk input must be a Buffer, readable stream, or iterable of byte values");
  }
  const hash = createHash(CHECKSUM_ALGORITHM);
  let sizeBytes = 0;
  const verifier = new Transform({
    transform(chunk, _encoding, callback) {
      if (!Buffer.isBuffer(chunk) && !(chunk instanceof Uint8Array)) {
        callback(new TypeError("Chunk input must yield Buffer or Uint8Array values"));
        return;
      }
      sizeBytes += chunk.length;
      hash.update(chunk);
      callback(null, chunk);
    }
  });
  await pipeline(source, verifier, createWriteStream(tempPath, { flags: "wx" }));
  return { sizeBytes, digest: hash.digest("hex") };
}

function corruptFirstByte() {
  let altered = false;
  return new Transform({
    transform(chunk, _encoding, callback) {
      if (!altered && chunk.length > 0) {
        const copy = Buffer.from(chunk);
        copy[0] ^= 0xff;
        altered = true;
        callback(null, copy);
        return;
      }
      callback(null, chunk);
    }
  });
}

function parseChunkId(chunkId) {
  if (typeof chunkId !== "string") throw new StorageError("INVALID_CHUNK_ID", "chunkId must be a string", { statusCode: 400 });
  const match = CHUNK_ID_PATTERN.exec(chunkId);
  if (!match) throw new StorageError("INVALID_CHUNK_ID", "chunkId must be chk_<sha256>", { statusCode: 400 });
  return match[1];
}

function checksum(value) {
  return Object.freeze({ algorithm: CHECKSUM_ALGORITHM, value });
}

function assertPositiveSize(value) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new StorageError("INVALID_LENGTH", "expectedSizeBytes must be a positive safe integer", { statusCode: 400 });
  }
}

function assertNodeId(value) {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/.test(value)) throw new TypeError("nodeId has an invalid format");
}

function safeDigestEquals(left, right) {
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
}

async function fileSizeOrNull(filePath) {
  try {
    return (await stat(filePath)).size;
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

async function unlinkIfPresent(filePath) {
  try {
    await unlink(filePath);
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}

function isPlainRecord(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
