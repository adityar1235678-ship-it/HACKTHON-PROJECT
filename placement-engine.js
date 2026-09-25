import { createHash } from "node:crypto";

import { PlacementError } from "./errors.js";

const CHUNK_ID_PATTERN = /^chk_[a-f0-9]{64}$/;

/**
 * Builds deterministic replica-placement plans from the current membership
 * view. A plan is advisory: the upload coordinator must still obtain the
 * policy's required write acknowledgements before committing its manifest.
 */
export class PlacementEngine {
  #metadata;

  constructor({ metadata }) {
    if (!metadata || typeof metadata.getDurabilityPolicy !== "function" || typeof metadata.listNodes !== "function") {
      throw new TypeError("metadata must provide getDurabilityPolicy() and listNodes()");
    }
    this.#metadata = metadata;
  }

  /**
   * Select additional write targets for a content-addressed chunk.
   * Existing replicas are never selected again, so the plan also serves
   * repairs and rebalancing without needing a special placement algorithm.
   */
  planWrite({
    chunkId,
    sizeBytes,
    policyId = "standard",
    existingReplicaNodeIds = [],
    allowDegraded = false
  }) {
    assertChunkId(chunkId);
    assertPositiveSize(sizeBytes);
    assertNodeIds(existingReplicaNodeIds);
    if (typeof allowDegraded !== "boolean") throw new TypeError("allowDegraded must be a boolean");

    const policy = this.#metadata.getDurabilityPolicy(policyId);
    const members = this.#metadata.listNodes({ includeRemoved: false });
    const memberById = new Map(members.map((node) => [node.nodeId, node]));
    const knownReplicaIds = [...new Set(existingReplicaNodeIds)];
    const durableExisting = knownReplicaIds.map((nodeId) => memberById.get(nodeId)).filter(isDurableReplica);
    const excludedNodeIds = new Set(knownReplicaIds);
    const candidates = members.filter((node) => !excludedNodeIds.has(node.nodeId) && canWrite(node, sizeBytes));
    const replicasNeeded = Math.max(0, policy.replicationFactor - durableExisting.length);
    const writeTargets = chooseFailureDomainDiverse(chunkId, candidates, replicasNeeded, durableExisting);
    const durableReplicaCount = durableExisting.length + writeTargets.length;
    const replicaDeficit = Math.max(0, policy.replicationFactor - durableReplicaCount);

    if (durableReplicaCount < policy.writeQuorum) {
      throw new PlacementError(
        "WRITE_QUORUM_UNAVAILABLE",
        `Placement can reach ${durableReplicaCount} replicas but policy ${policy.id} requires ${policy.writeQuorum} write acknowledgements`,
        { details: availabilityDetails(policy, candidates, durableExisting, sizeBytes) }
      );
    }
    if (replicaDeficit > 0 && !allowDegraded) {
      throw new PlacementError(
        "REPLICATION_UNSATISFIABLE",
        `Placement can reach ${durableReplicaCount} replicas but policy ${policy.id} requires ${policy.replicationFactor}`,
        { details: { ...availabilityDetails(policy, candidates, durableExisting, sizeBytes), replicaDeficit } }
      );
    }

    const targetReplicas = [...durableExisting, ...writeTargets];
    const requiredNewAcknowledgements = Math.max(0, policy.writeQuorum - durableExisting.length);
    return freezeClone({
      chunkId,
      sizeBytes,
      policy,
      currentDurableReplicaNodeIds: durableExisting.map((node) => node.nodeId),
      writeTargets: writeTargets.map(toTarget),
      targetNodeIds: targetReplicas.map((node) => node.nodeId),
      requiredWriteAcknowledgements: policy.writeQuorum,
      requiredNewAcknowledgements,
      desiredReplicaCount: policy.replicationFactor,
      replicaDeficit,
      isDegraded: replicaDeficit > 0,
      failureDomains: summarizeFailureDomains(targetReplicas)
    });
  }

  /** Return policy availability for a previously recorded set of replicas. */
  assessReplicaSet({ policyId = "standard", replicaNodeIds }) {
    assertNodeIds(replicaNodeIds);
    const policy = this.#metadata.getDurabilityPolicy(policyId);
    const nodesById = new Map(this.#metadata.listNodes({ includeRemoved: false }).map((node) => [node.nodeId, node]));
    const uniqueReplicaNodeIds = [...new Set(replicaNodeIds)];
    const durable = uniqueReplicaNodeIds.map((nodeId) => nodesById.get(nodeId)).filter(isDurableReplica);
    const unavailableNodeIds = uniqueReplicaNodeIds.filter((nodeId) => !durable.some((node) => node.nodeId === nodeId));
    return freezeClone({
      policy,
      replicaNodeIds: uniqueReplicaNodeIds,
      durableNodeIds: durable.map((node) => node.nodeId),
      unavailableNodeIds,
      durableReplicaCount: durable.length,
      replicaDeficit: Math.max(0, policy.replicationFactor - durable.length),
      writeQuorumAvailable: durable.length >= policy.writeQuorum,
      readQuorumAvailable: durable.length >= policy.readQuorum,
      failureDomains: summarizeFailureDomains(durable)
    });
  }
}

function chooseFailureDomainDiverse(chunkId, candidates, requiredCount, existing) {
  const remaining = [...candidates];
  const selected = [];
  const usedZones = new Set(existing.map(zoneKey));
  const usedRacks = new Set(existing.map(rackKey));

  while (selected.length < requiredCount && remaining.length > 0) {
    // New zones are the strongest protection against correlated failures.
    const distinctZone = remaining.filter((node) => !usedZones.has(zoneKey(node)));
    // When every available zone is already represented, avoid a duplicate rack.
    const distinctRack = remaining.filter((node) => !usedRacks.has(rackKey(node)));
    const pool = distinctZone.length > 0 ? distinctZone : distinctRack.length > 0 ? distinctRack : remaining;
    const winner = [...pool].sort((left, right) => compareRendezvous(chunkId, left, right))[0];
    selected.push(winner);
    usedZones.add(zoneKey(winner));
    usedRacks.add(rackKey(winner));
    remaining.splice(remaining.findIndex((node) => node.nodeId === winner.nodeId), 1);
  }
  return selected;
}

function compareRendezvous(chunkId, left, right) {
  const leftScore = rendezvousScore(chunkId, left);
  const rightScore = rendezvousScore(chunkId, right);
  if (leftScore > rightScore) return -1;
  if (leftScore < rightScore) return 1;
  return left.nodeId.localeCompare(right.nodeId);
}

function rendezvousScore(chunkId, node) {
  const hash = createHash("sha256").update(`${chunkId}\u0000${node.nodeId}`).digest("hex");
  const random = BigInt(`0x${hash.slice(0, 16)}`);
  const availableBytes = node.capacityBytes - node.usedBytes - node.reservedBytes;
  // Capacity weighting preserves a stable placement order while favoring
  // nodes that have proportionally more free space.
  return random * BigInt(availableBytes) / BigInt(node.capacityBytes);
}

function canWrite(node, sizeBytes) {
  return node?.state === "active" &&
    node.liveness === "healthy" &&
    node.reportedHealth === "healthy" &&
    node.capacityBytes - node.usedBytes - node.reservedBytes >= sizeBytes;
}

function isDurableReplica(node) {
  // A read-only node still holds a valid durable replica; it simply cannot be
  // chosen as a new placement target. Draining nodes are excluded so their
  // chunks are proactively moved before maintenance or removal.
  return node?.state === "active" &&
    node.liveness === "healthy" &&
    ["healthy", "read-only"].includes(node.reportedHealth);
}

function availabilityDetails(policy, candidates, existing, sizeBytes) {
  return {
    policy,
    sizeBytes,
    existingDurableNodeIds: existing.map((node) => node.nodeId),
    eligibleWriteNodeIds: candidates.map((node) => node.nodeId)
  };
}

function toTarget(node) {
  return {
    nodeId: node.nodeId,
    endpoint: node.endpoint,
    zone: node.zone,
    rack: node.rack,
    availableBytes: node.capacityBytes - node.usedBytes - node.reservedBytes
  };
}

function summarizeFailureDomains(nodes) {
  const zones = [...new Set(nodes.map(zoneKey))].sort();
  const racks = [...new Set(nodes.map(rackKey))].sort();
  return { zones, racks, uniqueZoneCount: zones.length, uniqueRackCount: racks.length };
}

function zoneKey(node) {
  return node.zone ?? `node:${node.nodeId}`;
}

function rackKey(node) {
  return `${zoneKey(node)}/${node.rack ?? `node:${node.nodeId}`}`;
}

function assertChunkId(value) {
  if (typeof value !== "string" || !CHUNK_ID_PATTERN.test(value)) throw new TypeError("chunkId must be chk_<sha256>");
}

function assertPositiveSize(value) {
  if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError("sizeBytes must be a positive safe integer");
}

function assertNodeIds(value) {
  if (!Array.isArray(value)) throw new TypeError("existingReplicaNodeIds/replicaNodeIds must be an array");
  for (const nodeId of value) {
    if (typeof nodeId !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/.test(nodeId)) {
      throw new TypeError("replica node ID has an invalid format");
    }
  }
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
