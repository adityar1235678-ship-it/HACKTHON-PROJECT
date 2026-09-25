import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";

import { ObjectApiError } from "./errors.js";

/** HTTP transport for the storage-node daemon, with per-request timeouts. */
export class HttpNodeTransport {
  #timeoutMs;

  constructor({ timeoutMs = 10_000 } = {}) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new TypeError("timeoutMs must be a positive safe integer");
    this.#timeoutMs = timeoutMs;
  }

  async putChunk(target, chunkId, bytes) {
    const response = await this.#request(target.endpoint, "PUT", `/v1/chunks/${encodeURIComponent(chunkId)}`, {
      headers: { "content-length": bytes.length, "content-type": "application/octet-stream" },
      body: bytes
    });
    if (response.statusCode !== 200 && response.statusCode !== 201) {
      throw await nodeResponseError(target.nodeId, response);
    }
    const payload = await readJson(response);
    if (payload.chunkId !== chunkId || payload.sizeBytes !== bytes.length || payload.checksum?.value !== chunkId.slice(4)) {
      throw new ObjectApiError("INVALID_NODE_RESPONSE", `Node ${target.nodeId} returned an invalid write acknowledgement`);
    }
    return Object.freeze({ nodeId: target.nodeId, created: payload.created, storedAt: new Date().toISOString() });
  }

  async getChunk(target, chunkId) {
    const response = await this.#request(target.endpoint, "GET", `/v1/chunks/${encodeURIComponent(chunkId)}`);
    if (response.statusCode !== 200) throw await nodeResponseError(target.nodeId, response);
    const sizeBytes = Number(response.headers["content-length"]);
    if (!Number.isSafeInteger(sizeBytes) || sizeBytes <= 0 || response.headers["x-vault-checksum"] !== chunkId.slice(4)) {
      response.resume();
      throw new ObjectApiError("INVALID_NODE_RESPONSE", `Node ${target.nodeId} returned invalid chunk headers`);
    }
    return Object.freeze({ nodeId: target.nodeId, sizeBytes, stream: response });
  }

  #request(endpoint, method, path, { headers = {}, body } = {}) {
    let url;
    try { url = new URL(endpoint); } catch { throw new ObjectApiError("INVALID_NODE_ENDPOINT", `Node endpoint ${endpoint} is invalid`); }
    const transport = url.protocol === "https:" ? httpsRequest : url.protocol === "http:" ? httpRequest : null;
    if (!transport) throw new ObjectApiError("INVALID_NODE_ENDPOINT", `Node endpoint ${endpoint} must use HTTP or HTTPS`);

    return new Promise((resolve, reject) => {
      const request = transport({
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port,
        method,
        path,
        headers
      }, resolve);
      request.setTimeout(this.#timeoutMs, () => request.destroy(new ObjectApiError("NODE_TIMEOUT", `Timed out contacting ${endpoint}`)));
      request.on("error", (error) => reject(error instanceof ObjectApiError ? error : new ObjectApiError("NODE_UNREACHABLE", `Unable to contact ${endpoint}`, { cause: error })));
      if (body) request.end(body); else request.end();
    });
  }
}

async function nodeResponseError(nodeId, response) {
  let code = "NODE_OPERATION_FAILED";
  let message = `Node ${nodeId} returned HTTP ${response.statusCode}`;
  try {
    const payload = await readJson(response);
    code = payload?.error?.code ?? code;
    message = payload?.error?.message ?? message;
  } catch {
    response.resume();
  }
  return new ObjectApiError(code, message, { details: { nodeId, statusCode: response.statusCode } });
}

async function readJson(stream) {
  const buffers = [];
  for await (const bytes of stream) buffers.push(bytes);
  try { return JSON.parse(Buffer.concat(buffers).toString("utf8")); } catch { throw new ObjectApiError("INVALID_NODE_RESPONSE", "Storage node returned invalid JSON"); }
}
