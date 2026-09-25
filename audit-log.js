import { appendFile, mkdir, readFile } from "node:fs/promises";
import { dirname } from "node:path";

/** Append-only JSON Lines audit trail for operational and recovery actions. */
export class AuditLog {
  #filePath;
  #clock;
  #lock = Promise.resolve();

  constructor({ filePath, clock = () => Date.now() }) {
    if (typeof filePath !== "string" || filePath.length === 0) throw new TypeError("filePath must be a non-empty path");
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    this.#filePath = filePath;
    this.#clock = clock;
  }

  async append({ actor = "system", action, target, outcome = "success", details = {} }) {
    if (typeof actor !== "string" || actor.length === 0 || actor.length > 128) throw new TypeError("audit actor must be a short non-empty string");
    if (typeof action !== "string" || !/^[A-Z][A-Z0-9_.-]{1,127}$/.test(action)) throw new TypeError("audit action has an invalid format");
    if (typeof target !== "string" || target.length === 0 || target.length > 512) throw new TypeError("audit target must be a short non-empty string");
    if (!new Set(["success", "failure", "deferred"]).has(outcome)) throw new TypeError("audit outcome is invalid");
    assertJsonObject(details);
    const timestamp = new Date(this.#clock()).toISOString();
    const entry = { timestamp, actor, action, target, outcome, details: structuredClone(details) };
    const previous = this.#lock;
    let release;
    this.#lock = new Promise((resolve) => { release = resolve; });
    await previous;
    try {
      await mkdir(dirname(this.#filePath), { recursive: true });
      await appendFile(this.#filePath, `${JSON.stringify(entry)}\n`, "utf8");
      return freezeClone(entry);
    } finally {
      release();
    }
  }

  async read({ limit = 100 } = {}) {
    if (!Number.isSafeInteger(limit) || limit < 0 || limit > 10_000) throw new TypeError("audit limit must be an integer from 0 to 10000");
    let raw;
    try { raw = await readFile(this.#filePath, "utf8"); } catch (error) { if (error?.code === "ENOENT") return []; throw error; }
    const lines = raw.split("\n").filter(Boolean);
    const records = lines.map((line) => {
      try { return JSON.parse(line); } catch { throw new Error("Audit log contains invalid JSON Lines data"); }
    });
    return freezeClone(records.slice(-limit));
  }
}

function assertJsonObject(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("audit details must be a JSON object");
  try { JSON.stringify(value); } catch { throw new TypeError("audit details must be JSON-compatible"); }
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
