# Vault

Vault is a fault-tolerant distributed object-storage system. This repository currently implements the first two foundations: the **core object model** and a local **storage-node daemon**. It has no third-party runtime dependencies.

## What is implemented

- Immutable, versioned object manifests
- Streaming **fixed-size** chunking that never buffers a full object
- SHA-256 checksums for chunks, complete object content, and the manifest itself
- Content-addressed chunk IDs (`chk_<sha256>`)
- Replica-location records for mapping chunks to storage nodes
- Strict validation that detects malformed manifests, duplicate replica locations, non-contiguous chunks, and checksum tampering
- Draft manifests for placement planning and committed manifests that require at least one replica for every chunk
- Storage-node HTTP API for chunk reads, writes, deletes, health, capacity, and fault simulation
- Atomic disk writes and checksum verification before chunks become visible

## Object format

```text
Object manifest
├── objectId / version / createdAt / state
├── contentChecksum      SHA-256 of all original object bytes
├── chunking             { strategy: "fixed", chunkSizeBytes }
├── chunks[]
│   ├── index / sizeBytes
│   ├── chunkId           chk_<SHA-256 of chunk bytes>
│   ├── checksum
│   └── replicas[]        { nodeId, state, storedAt? }
└── manifestChecksum      SHA-256 of canonical manifest content
```

The `manifestChecksum` is calculated from canonical JSON with keys sorted recursively, ensuring independent processes compute the same value for the same manifest. It is excluded from its own input.

## Design choices

Vault starts with fixed-size chunks because it makes streaming, repair, balancing, and capacity calculations predictable. A later content-defined chunker can use the same `ChunkDescriptor` and `ObjectManifest` structures; only the `chunking.strategy` and the boundary generator need change.

The same content-addressed chunk ID may appear at several object offsets. That is valid and leaves room for a later chunk-deduplication layer to store that byte sequence once while each manifest retains its original ordering.

Writes initially create a `draft` manifest, which may have no assigned replicas. The placement/replication layer will convert it to `committed` only after every chunk has enough durable replicas. This prevents metadata from claiming durability before it exists.

## Try it

```js
import { buildManifestFromStream, validateManifest } from "./src/index.js";

const input = [Buffer.from("hello "), Buffer.from("vault")];
const manifest = await buildManifestFromStream(input, {
  objectId: "obj_demo",
  chunkSizeBytes: 4,
  state: "committed",
  placementForChunk: ({ index }) => [
    { nodeId: `node-${index % 2}`, state: "healthy" }
  ]
});

validateManifest(manifest);
console.log(manifest);
```

Run checks with:

```bash
npm test
npm run check
```

If only the Node runtime is available, run the test suite directly with `node --test --test-isolation=none`.

## Run a storage node

```bash
VAULT_NODE_ID=node-a VAULT_DATA_DIR=./data/node-a VAULT_PORT=8081 npm run start:node
```

The node exposes `PUT`, `GET`, `HEAD`, and `DELETE /v1/chunks/:chunkId`, as well as `GET /v1/health`, `GET /v1/stats`, and `POST /v1/admin/faults`. A chunk ID must be the content address `chk_<sha256>`. `PUT` requires an accurate `Content-Length`; the node reserves capacity, streams to a private temporary file, validates both length and SHA-256, then atomically publishes the file.

Example fault injection payload:

```json
{ "online": false }
```

Supported fault controls are `online`, `writable`, `partitioned`, `latencyMs`, and `corruptReads`. Fault injection is intentionally an administrative, local-development API and must be protected or omitted in a production deployment.
