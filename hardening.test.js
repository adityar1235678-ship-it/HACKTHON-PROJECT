import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  AuditLog,
  createStorageNodeServer,
  HttpNodeTransport,
  IntegrityScrubber,
  MetadataStore,
  MetricsRegistry,
  NodeReconciler,
  ObjectApi,
  OperationsController,
  OrphanCleaner,
  PlacementEngine,
  RebalancePlanner,
  RebalanceQueue,
  RebalanceWorker,
  RepairQueue,
  RepairWorker,
  StorageNode
} from "../src/index.js";

async function withChaosCluster(callback) {
  const rootDir = await mkdtemp(join(tmpdir(), "vault-hardening-test-"));
  const metadata = await new MetadataStore({ filePath: join(rootDir, "metadata.json") }).initialize();
  const nodes = [];
  try {
    await metadata.upsertDurabilityPolicy({ id: "hardened", replicationFactor: 3, writeQuorum: 2, readQuorum: 2 });
    for (let index = 0; index < 4; index += 1) {
      const nodeId = `node_${index + 1}`;
      const dataDir = join(rootDir, nodeId);
      const node = await new StorageNode({ nodeId, rootDir: dataDir, capacityBytes: 5_000_000 }).initialize();
      const server = createStorageNodeServer(node, { adminToken: "chaos-token" });
      await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
      const endpoint = `http://127.0.0.1:${server.address().port}`;
      nodes.push({ nodeId, node, server, endpoint, dataDir });
      await metadata.registerNode({ nodeId, endpoint, capacityBytes: 5_000_000, zone: `zone_${index + 1}`, rack: "rack_1" });
      await metadata.heartbeat(nodeId, { usedBytes: 0, reservedBytes: 0, capacityBytes: 5_000_000, reportedHealth: "healthy" });
    }
    const transport = new HttpNodeTransport({ adminToken: "chaos-token" });
    const placement = new PlacementEngine({ metadata });
    const repairQueue = new RepairQueue();
    const rebalanceQueue = new RebalanceQueue();
    const api = new ObjectApi({ metadata, placement, transport, repairQueue, chunkSizeBytes: 17 });
    const scrubber = new IntegrityScrubber({ metadata, placement, transport, repairQueue });
    const repairWorker = new RepairWorker({ metadata, placement, transport, repairQueue });
    const rebalancePlanner = new RebalancePlanner({ metadata, placement, queue: rebalanceQueue });
    const rebalanceWorker = new RebalanceWorker({ metadata, placement, transport, queue: rebalanceQueue });
    const reconciler = new NodeReconciler({ metadata, transport, repairQueue });
    const orphanCleaner = new OrphanCleaner({ metadata, placement, transport, gracePeriodMs: 1_000_000 });
    const controller = new OperationsController({
      metadata,
      transport,
      scrubber,
      repairWorker,
      rebalancePlanner,
      rebalanceWorker,
      reconciler,
      orphanCleaner,
      repairQueue,
      rebalanceQueue,
      metrics: new MetricsRegistry(),
      auditLog: new AuditLog({ filePath: join(rootDir, "audit.jsonl") })
    });
    return await callback({ metadata, nodes, api, controller, transport });
  } finally {
    await Promise.all(nodes.map(({ server }) => {
      server.closeAllConnections?.();
      return new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }));
    await rm(rootDir, { recursive: true, force: true });
  }
}

test("concurrent streamed uploads converge to full replication and remain readable through one-node partition", async () => {
  await withChaosCluster(async ({ metadata, nodes, api, controller }) => {
    const objects = Array.from({ length: 6 }, (_value, index) => ({
      objectId: `obj_parallel_${index}`,
      version: "ver_1",
      body: Buffer.from(`concurrent-object-${index}|`.repeat(18))
    }));
    const uploaded = await Promise.all(objects.map(({ objectId, version, body }) => api.putObject(fragmented(body), {
      objectId,
      version,
      policyId: "hardened",
      idempotencyKey: `parallel-${objectId}`
    })));
    assert.equal(uploaded.length, objects.length);

    await controller.runMaintenanceCycle({ maxRecoveryNodes: 0, maxScrubChunks: 1_000, maxRepairJobs: 1_000, maxRebalanceJobs: 0, orphanNodeIds: [] });
    for (const expected of objects) {
      const manifest = metadata.getManifest(expected.objectId, { version: "ver_1" });
      for (const chunk of manifest.chunks) {
        const replicaSet = metadata.getReplicaSet(expected.objectId, { version: "ver_1", chunkId: chunk.chunkId });
        assert.equal(replicaSet.replicas.filter((replica) => replica.state === "healthy").length, 3);
      }
    }

    const firstManifest = metadata.getManifest(objects[0].objectId, { version: "ver_1" });
    const partitionedNodeId = metadata.getReplicaSet(objects[0].objectId, { version: "ver_1", chunkId: firstManifest.chunks[0].chunkId }).replicas[0].nodeId;
    await controller.simulateNodeFault(partitionedNodeId, { online: false }, { actor: "chaos" });
    for (const expected of objects) {
      const result = await api.getObject(expected.objectId);
      assert.deepEqual(await collect(result.stream), expected.body);
      await result.completion;
    }
    await controller.simulateNodeFault(partitionedNodeId, { online: true }, { actor: "chaos" });
    assert.equal(nodes.find((node) => node.nodeId === partitionedNodeId).node.health().reachable, true);
  });
});

test("version races and a three-node partition cannot overwrite an established object version", async () => {
  await withChaosCluster(async ({ metadata, nodes, api, controller }) => {
    await api.putObject(Buffer.from("initial version"), { objectId: "obj_race_hardened", version: "ver_1", policyId: "hardened" });
    const left = api.putObject(Buffer.from("left update"), {
      objectId: "obj_race_hardened", version: "ver_2", policyId: "hardened", expectedCurrentVersion: "ver_1"
    });
    const right = api.putObject(Buffer.from("right update"), {
      objectId: "obj_race_hardened", version: "ver_3", policyId: "hardened", expectedCurrentVersion: "ver_1"
    });
    const outcomes = await Promise.allSettled([left, right]);
    const winner = outcomes.find((outcome) => outcome.status === "fulfilled").value;
    assert.equal(outcomes.filter((outcome) => outcome.status === "fulfilled").length, 1);
    assert.equal(outcomes.find((outcome) => outcome.status === "rejected").reason.code, "VERSION_CONFLICT");

    const offline = nodes.slice(0, 3);
    for (const node of offline) await controller.simulateNodeFault(node.nodeId, { online: false }, { actor: "chaos" });
    await assert.rejects(
      () => api.putObject(Buffer.from("must not publish"), {
        objectId: "obj_race_hardened",
        version: "ver_4",
        policyId: "hardened",
        expectedCurrentVersion: winner.version
      }),
      { code: "WRITE_QUORUM_NOT_ACKNOWLEDGED" }
    );
    assert.equal(metadata.getObject("obj_race_hardened").currentVersion, winner.version);
    for (const node of offline) await controller.simulateNodeFault(node.nodeId, { online: true }, { actor: "chaos" });

    const result = await api.getObject("obj_race_hardened", { version: winner.version });
    assert.ok(["left update", "right update"].includes((await collect(result.stream)).toString()));
    await result.completion;
  });
});

test("scrub, repair, and rebalance converge through simultaneous corruption and node drain", async () => {
  await withChaosCluster(async ({ metadata, nodes, api, controller }) => {
    const body = Buffer.from("corruption plus node churn");
    const uploaded = await api.putObject(body, { objectId: "obj_churn", version: "ver_1", policyId: "hardened" });
    await controller.runMaintenanceCycle({ maxRecoveryNodes: 0, maxScrubChunks: 100, maxRepairJobs: 100, maxRebalanceJobs: 0, orphanNodeIds: [] });
    const chunk = uploaded.manifest.chunks[0];
    const before = metadata.getReplicaSet("obj_churn", { version: "ver_1", chunkId: chunk.chunkId }).replicas.filter((replica) => replica.state === "healthy");
    const corruptNode = nodes.find((node) => node.nodeId === before[0].nodeId);
    const drainingNodeId = before[1].nodeId;
    await writeFile(join(corruptNode.dataDir, "chunks", chunk.chunkId.slice(4, 6), chunk.chunkId), Buffer.alloc(chunk.sizeBytes, 0x5a));
    await controller.drainNode(drainingNodeId, { actor: "chaos" });

    const maintenance = await controller.runMaintenanceCycle({ maxRecoveryNodes: 0, maxScrubChunks: 100, maxRepairJobs: 100, maxRebalanceJobs: 100, orphanNodeIds: [] });
    assert.equal(maintenance.scrub.failedReplicas, 1);
    assert.ok(maintenance.repairs.some((result) => result.status === "repaired"));
    assert.ok(maintenance.rebalances.some((result) => ["moved", "retired"].includes(result.status)), JSON.stringify(maintenance.rebalances));
    const after = metadata.getReplicaSet("obj_churn", { version: "ver_1", chunkId: chunk.chunkId }).replicas;
    assert.equal(after.filter((replica) => replica.state === "healthy").length, 3);
    assert.equal(after.find((replica) => replica.nodeId === drainingNodeId).state, "stale");

    const fetched = await api.getObject("obj_churn");
    assert.deepEqual(await collect(fetched.stream), body);
    await fetched.completion;
  });
});

async function* fragmented(bytes) {
  const sizes = [1, 5, 2, 11, 3, 7];
  let offset = 0;
  let index = 0;
  while (offset < bytes.length) {
    const next = Math.min(bytes.length, offset + sizes[index % sizes.length]);
    yield bytes.subarray(offset, next);
    offset = next;
    index += 1;
    await Promise.resolve();
  }
}

async function collect(stream) {
  const buffers = [];
  for await (const bytes of stream) buffers.push(bytes);
  return Buffer.concat(buffers);
}
