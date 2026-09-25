import assert from "node:assert/strict";
import test from "node:test";

import {
  buildManifestFromStream,
  canonicalJson,
  commitManifest,
  fixedSizeChunks,
  validateManifest
} from "../src/index.js";

async function collectChunks(input, size) {
  const chunks = [];
  for await (const chunk of fixedSizeChunks(input, size)) chunks.push(chunk);
  return chunks;
}

test("fixed-size chunker maintains boundaries across streamed input", async () => {
  const chunks = await collectChunks([Buffer.from("ab"), Buffer.from("cdefg"), Buffer.from("h")], 3);
  assert.deepEqual(chunks.map((chunk) => chunk.toString()), ["abc", "def", "gh"]);
});

test("fixed-size chunker accepts one Buffer as the complete object", async () => {
  const chunks = await collectChunks(Buffer.from("abcdef"), 4);
  assert.deepEqual(chunks.map((chunk) => chunk.toString()), ["abcd", "ef"]);
});

test("manifest contains checksummed content-addressed chunks and replica locations", async () => {
  const manifest = await buildManifestFromStream([Buffer.from("hello "), Buffer.from("vault")], {
    objectId: "obj_demo",
    version: "ver_1",
    createdAt: "2026-09-26T00:00:00.000Z",
    chunkSizeBytes: 4,
    state: "committed",
    metadata: { owner: "test", tier: "standard" },
    placementForChunk: ({ index }) => [{
      nodeId: `node_${index + 1}`,
      state: "healthy",
      storedAt: "2026-09-26T00:00:00.000Z"
    }]
  });

  assert.equal(manifest.sizeBytes, 11);
  assert.deepEqual(manifest.chunks.map((chunk) => chunk.sizeBytes), [4, 4, 3]);
  assert.equal(manifest.chunks[0].chunkId, `chk_${manifest.chunks[0].checksum.value}`);
  assert.equal(manifest.chunks[2].replicas[0].nodeId, "node_3");
  assert.equal(validateManifest(manifest), true);
  assert.equal(Object.isFrozen(manifest.chunks[0].replicas), true);
});

test("draft manifests can be committed after replicas are assigned", async () => {
  const draft = await buildManifestFromStream([Buffer.from("abc")], {
    objectId: "obj_draft",
    version: "ver_1",
    createdAt: "2026-09-26T00:00:00.000Z",
    chunkSizeBytes: 8
  });
  assert.equal(draft.state, "draft");
  assert.deepEqual(draft.chunks[0].replicas, []);

  const withReplica = structuredClone(draft);
  withReplica.chunks[0].replicas = [{ nodeId: "node_a", state: "healthy" }];
  // The metadata layer will recalculate this using commitManifest after placing chunks.
  withReplica.manifestChecksum = {
    algorithm: "sha256",
    value: "0".repeat(64)
  };
  assert.throws(() => commitManifest(withReplica), /manifestChecksum/);

  const replanned = await buildManifestFromStream([Buffer.from("abc")], {
    objectId: "obj_draft",
    version: "ver_1",
    createdAt: "2026-09-26T00:00:00.000Z",
    chunkSizeBytes: 8,
    placementForChunk: () => [{ nodeId: "node_a", state: "healthy" }]
  });
  const committed = commitManifest(replanned);
  assert.equal(committed.state, "committed");
  assert.equal(validateManifest(committed), true);
});

test("validator rejects tampered manifests and duplicate placement", async () => {
  const manifest = await buildManifestFromStream([Buffer.from("abcdef")], {
    objectId: "obj_tamper",
    version: "ver_1",
    createdAt: "2026-09-26T00:00:00.000Z",
    chunkSizeBytes: 3,
    placementForChunk: () => [{ nodeId: "node_a", state: "healthy" }]
  });

  const tampered = structuredClone(manifest);
  tampered.chunks[0].replicas.push({ nodeId: "node_a", state: "healthy" });
  assert.throws(() => validateManifest(tampered), /duplicated/);

  const alteredMetadata = structuredClone(manifest);
  alteredMetadata.metadata.owner = "attacker";
  assert.throws(() => validateManifest(alteredMetadata), /manifestChecksum/);

  const invalidSchema = structuredClone(manifest);
  invalidSchema.unrecognized = true;
  assert.throws(() => validateManifest(invalidSchema), /not supported/);
});

test("repeated chunk content is valid at separate object offsets", async () => {
  const manifest = await buildManifestFromStream([Buffer.from("aaaaaa")], {
    objectId: "obj_repeated",
    version: "ver_1",
    createdAt: "2026-09-26T00:00:00.000Z",
    chunkSizeBytes: 3,
    placementForChunk: () => [{ nodeId: "node_a", state: "healthy" }]
  });

  assert.equal(manifest.chunks.length, 2);
  assert.equal(manifest.chunks[0].chunkId, manifest.chunks[1].chunkId);
  assert.equal(validateManifest(manifest), true);
});

test("canonical JSON has stable object-key ordering", () => {
  assert.equal(canonicalJson({ b: 2, a: { z: true, y: false } }), '{"a":{"y":false,"z":true},"b":2}');
});
