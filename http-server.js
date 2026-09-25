import { createServer } from "node:http";
import { pipeline } from "node:stream/promises";

import { asStorageError, StorageError } from "./errors.js";

/** Create the HTTP surface for one initialized StorageNode. */
export function createStorageNodeServer(node, { adminToken } = {}) {
  if (!node || typeof node.putChunk !== "function") throw new TypeError("node must be an initialized StorageNode");

  return createServer(async (request, response) => {
    try {
      await handleRequest(node, request, response, { adminToken });
    } catch (error) {
      if (response.headersSent) {
        response.destroy(error);
        return;
      }
      const storageError = asStorageError(error);
      sendJson(response, storageError.statusCode, { error: { code: storageError.code, message: storageError.message } });
    }
  });
}

async function handleRequest(node, request, response, { adminToken }) {
  const url = new URL(request.url, "http://vault-node.local");
  const chunkId = chunkIdFromPath(url.pathname);

  if (request.method === "GET" && url.pathname === "/v1/health") return sendJson(response, 200, node.health());
  if (request.method === "GET" && url.pathname === "/v1/stats") return sendJson(response, 200, node.stats());
  if (request.method === "GET" && url.pathname === "/v1/chunks") return sendJson(response, 200, { chunkIds: await node.listChunkIds() });

  if (url.pathname === "/v1/admin/faults") {
    assertAdminAuthorized(request, adminToken);
    if (request.method === "GET") return sendJson(response, 200, node.health());
    if (request.method === "POST") return sendJson(response, 200, node.setFaults(await readJson(request)));
  }

  if (chunkId) {
    if (request.method === "PUT") {
      const result = await node.putChunk(chunkId, request, { expectedSizeBytes: contentLength(request) });
      return sendJson(response, result.created ? 201 : 200, result);
    }
    if (request.method === "GET") {
      const result = await node.getChunk(chunkId);
      response.writeHead(200, {
        "content-type": "application/octet-stream",
        "content-length": result.sizeBytes,
        "x-vault-checksum-algorithm": result.checksum.algorithm,
        "x-vault-checksum": result.checksum.value
      });
      await pipeline(result.stream, response);
      return;
    }
    if (request.method === "HEAD") {
      const result = await node.headChunk(chunkId);
      response.writeHead(200, {
        "content-length": result.sizeBytes,
        "x-vault-checksum-algorithm": result.checksum.algorithm,
        "x-vault-checksum": result.checksum.value
      });
      response.end();
      return;
    }
    if (request.method === "DELETE") return sendJson(response, 200, await node.deleteChunk(chunkId));
  }

  throw new StorageError("NOT_FOUND", "Route not found", { statusCode: 404 });
}

function chunkIdFromPath(pathname) {
  const match = /^\/v1\/chunks\/([^/]+)$/.exec(pathname);
  if (!match) return null;
  try {
    return decodeURIComponent(match[1]);
  } catch {
    throw new StorageError("INVALID_CHUNK_ID", "Chunk path is not valid URL encoding", { statusCode: 400 });
  }
}

function contentLength(request) {
  const value = request.headers["content-length"];
  if (typeof value !== "string" || !/^[1-9]\d*$/.test(value)) {
    throw new StorageError("LENGTH_REQUIRED", "PUT requests require a positive Content-Length", { statusCode: 411 });
  }
  const size = Number(value);
  if (!Number.isSafeInteger(size)) throw new StorageError("INVALID_LENGTH", "Content-Length is too large", { statusCode: 400 });
  return size;
}

async function readJson(request) {
  const maxBytes = 64 * 1024;
  const buffers = [];
  let sizeBytes = 0;
  for await (const chunk of request) {
    sizeBytes += chunk.length;
    if (sizeBytes > maxBytes) throw new StorageError("PAYLOAD_TOO_LARGE", "Administrative request body is too large", { statusCode: 413 });
    buffers.push(chunk);
  }
  try {
    const parsed = JSON.parse(Buffer.concat(buffers).toString("utf8"));
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("not an object");
    return parsed;
  } catch {
    throw new StorageError("INVALID_JSON", "Request body must be a JSON object", { statusCode: 400 });
  }
}

function assertAdminAuthorized(request, adminToken) {
  if (adminToken && request.headers.authorization !== `Bearer ${adminToken}`) {
    throw new StorageError("UNAUTHORIZED", "Administrative authorization is required", { statusCode: 401 });
  }
}

function sendJson(response, statusCode, body) {
  const encoded = JSON.stringify(body);
  response.writeHead(statusCode, { "content-type": "application/json", "content-length": Buffer.byteLength(encoded) });
  response.end(encoded);
}
