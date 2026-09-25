/**
 * Coordinates operational controls and a bounded maintenance pass. It keeps
 * all changes explicit and audited while leaving individual workers usable as
 * independently scheduled components.
 */
export class OperationsController {
  #metadata;
  #transport;
  #scrubber;
  #repairWorker;
  #rebalancePlanner;
  #rebalanceWorker;
  #reconciler;
  #orphanCleaner;
  #repairQueue;
  #rebalanceQueue;
  #metrics;
  #auditLog;
  #clock;

  constructor({
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
    metrics,
    auditLog,
    clock = () => Date.now()
  }) {
    if (!metadata || typeof metadata.setNodeState !== "function" || typeof metadata.upsertDurabilityPolicy !== "function" || typeof metadata.listNodes !== "function" || typeof metadata.snapshot !== "function" || typeof metadata.getNode !== "function" || typeof metadata.reconcileNodeLiveness !== "function") {
      throw new TypeError("metadata must support operational updates and snapshots");
    }
    if (!transport || typeof transport.setFaults !== "function") throw new TypeError("transport must support setFaults()");
    assertWorker("scrubber", scrubber, "scrub");
    assertWorker("repairWorker", repairWorker, "runBatch");
    assertWorker("rebalancePlanner", rebalancePlanner, "plan");
    assertWorker("rebalanceWorker", rebalanceWorker, "runBatch");
    assertWorker("reconciler", reconciler, "antiEntropy");
    assertWorker("orphanCleaner", orphanCleaner, "scanNode");
    if (!repairQueue || typeof repairQueue.size !== "number") throw new TypeError("repairQueue must expose size");
    if (!rebalanceQueue || typeof rebalanceQueue.size !== "number") throw new TypeError("rebalanceQueue must expose size");
    if (!metrics || typeof metrics.increment !== "function" || typeof metrics.setGauge !== "function" || typeof metrics.observe !== "function") {
      throw new TypeError("metrics must be a MetricsRegistry-compatible instance");
    }
    if (!auditLog || typeof auditLog.append !== "function") throw new TypeError("auditLog must support append()");
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    this.#metadata = metadata;
    this.#transport = transport;
    this.#scrubber = scrubber;
    this.#repairWorker = repairWorker;
    this.#rebalancePlanner = rebalancePlanner;
    this.#rebalanceWorker = rebalanceWorker;
    this.#reconciler = reconciler;
    this.#orphanCleaner = orphanCleaner;
    this.#repairQueue = repairQueue;
    this.#rebalanceQueue = rebalanceQueue;
    this.#metrics = metrics;
    this.#auditLog = auditLog;
    this.#clock = clock;
  }

  async drainNode(nodeId, { actor = "operator", maxRebalanceJobs = 1_000 } = {}) {
    return this.#audited(actor, "NODE.DRAIN", nodeId, async () => {
      const node = await this.#metadata.setNodeState(nodeId, "draining");
      const plan = await this.#rebalancePlanner.plan({ maxJobs: maxRebalanceJobs });
      this.#metrics.increment("vault_node_drains_total");
      await this.#refreshGauges();
      return { node, rebalancePlan: plan };
    });
  }

  async activateNode(nodeId, { actor = "operator" } = {}) {
    return this.#audited(actor, "NODE.ACTIVATE", nodeId, async () => {
      const node = await this.#metadata.setNodeState(nodeId, "active");
      this.#metrics.increment("vault_node_activations_total");
      await this.#refreshGauges();
      return node;
    });
  }

  async changeDurabilityPolicy(policy, { actor = "operator" } = {}) {
    return this.#audited(actor, "POLICY.UPSERT", policy?.id ?? "unknown", async () => {
      const saved = await this.#metadata.upsertDurabilityPolicy(policy);
      this.#metrics.increment("vault_policy_changes_total", 1, { policy: saved.id });
      return saved;
    });
  }

  async simulateNodeFault(nodeId, faults, { actor = "operator" } = {}) {
    return this.#audited(actor, "NODE.FAULT_SIMULATION", nodeId, async () => {
      const node = this.#metadata.getNode(nodeId);
      const health = await this.#transport.setFaults(node, faults);
      this.#metrics.increment("vault_fault_simulations_total");
      return health;
    });
  }

  /** Run an intentionally bounded, observable background maintenance pass. */
  async runMaintenanceCycle({
    actor = "system",
    maxRecoveryNodes = 100,
    maxScrubChunks = 10_000,
    maxRepairJobs = 1_000,
    maxRebalanceJobs = 1_000,
    orphanNodeIds
  } = {}) {
    const startedAt = this.#clock();
    try {
      const liveness = await this.#metadata.reconcileNodeLiveness();
      const recovery = await this.#reconciler.antiEntropy({ maxNodes: maxRecoveryNodes });
      const scrub = await this.#scrubber.scrub({ maxChunks: maxScrubChunks });
      const repairs = await this.#repairWorker.runBatch({ maxJobs: maxRepairJobs });
      const rebalancePlan = await this.#rebalancePlanner.plan({ maxJobs: maxRebalanceJobs });
      const rebalances = await this.#rebalanceWorker.runBatch({ maxJobs: maxRebalanceJobs });
      const candidates = orphanNodeIds ?? this.#metadata.listNodes({ includeRemoved: false }).map((node) => node.nodeId);
      if (!Array.isArray(candidates)) throw new TypeError("orphanNodeIds must be an array when provided");
      const orphans = [];
      for (const nodeId of candidates) orphans.push(await this.#orphanCleaner.scanNode(nodeId));
      const durationMs = this.#clock() - startedAt;
      const result = { liveness, recovery, scrub, repairs, rebalancePlan, rebalances, orphans, durationMs };
      this.#recordMaintenanceMetrics(result);
      await this.#refreshGauges();
      await this.#auditLog.append({ actor, action: "MAINTENANCE.CYCLE", target: "cluster", details: compactCycleResult(result) });
      return freezeClone(result);
    } catch (error) {
      this.#metrics.increment("vault_maintenance_failures_total");
      await this.#auditLog.append({
        actor,
        action: "MAINTENANCE.CYCLE",
        target: "cluster",
        outcome: "failure",
        details: { code: error?.code ?? "MAINTENANCE_FAILED", message: error?.message ?? "Maintenance cycle failed" }
      });
      throw error;
    }
  }

  async status() {
    await this.#refreshGauges();
    return freezeClone({
      cluster: this.#metadata.snapshot(),
      repairQueueDepth: this.#repairQueue.size,
      rebalanceQueueDepth: this.#rebalanceQueue.size,
      metrics: this.#metrics.snapshot()
    });
  }

  async #audited(actor, action, target, operation) {
    try {
      const result = await operation();
      await this.#auditLog.append({ actor, action, target, details: summarizeResult(result) });
      return freezeClone(result);
    } catch (error) {
      this.#metrics.increment("vault_operational_failures_total", 1, { action });
      await this.#auditLog.append({
        actor,
        action,
        target,
        outcome: "failure",
        details: { code: error?.code ?? "OPERATION_FAILED", message: error?.message ?? "Operational command failed" }
      });
      throw error;
    }
  }

  #recordMaintenanceMetrics(result) {
    this.#metrics.increment("vault_maintenance_cycles_total");
    this.#metrics.increment("vault_scrub_chunks_total", result.scrub.scannedChunks);
    this.#metrics.increment("vault_scrub_replica_failures_total", result.scrub.failedReplicas);
    for (const repair of result.repairs) this.#metrics.increment("vault_repair_jobs_total", 1, { status: repair.status });
    for (const rebalance of result.rebalances) this.#metrics.increment("vault_rebalance_jobs_total", 1, { status: rebalance.status });
    this.#metrics.increment("vault_orphan_chunks_deleted_total", result.orphans.reduce((sum, entry) => sum + (entry.deleted ?? 0), 0));
    this.#metrics.observe("vault_maintenance_cycle_duration_ms", result.durationMs);
  }

  async #refreshGauges() {
    const nodes = this.#metadata.listNodes({ includeRemoved: true });
    const activeNodes = nodes.filter((node) => node.state !== "removed");
    this.#metrics.setGauge("vault_nodes_total", activeNodes.length);
    this.#metrics.setGauge("vault_nodes_healthy", activeNodes.filter((node) => node.liveness === "healthy" && node.reportedHealth === "healthy" && node.state === "active").length);
    this.#metrics.setGauge("vault_nodes_unreachable", activeNodes.filter((node) => node.liveness === "unreachable").length);
    this.#metrics.setGauge("vault_storage_used_bytes", activeNodes.reduce((sum, node) => sum + node.usedBytes, 0));
    this.#metrics.setGauge("vault_storage_available_bytes", activeNodes.reduce((sum, node) => sum + node.capacityBytes - node.usedBytes - node.reservedBytes, 0));
    this.#metrics.setGauge("vault_repair_queue_depth", this.#repairQueue.size);
    this.#metrics.setGauge("vault_rebalance_queue_depth", this.#rebalanceQueue.size);
    this.#metrics.setGauge("vault_orphan_candidate_depth", this.#metadata.snapshot().orphanCandidateCount);
  }
}

function assertWorker(name, value, method) {
  if (!value || typeof value[method] !== "function") throw new TypeError(`${name} must support ${method}()`);
}

function summarizeResult(result) {
  if (result && typeof result === "object") {
    if (result.nodeId) return { nodeId: result.nodeId, state: result.state, status: result.status };
    if (result.id) return { id: result.id };
  }
  return {};
}

function compactCycleResult(result) {
  return {
    durationMs: result.durationMs,
    scrubbedChunks: result.scrub.scannedChunks,
    scrubFailures: result.scrub.failedReplicas,
    repairJobs: result.repairs.length,
    rebalanceJobs: result.rebalances.length,
    orphanDeletes: result.orphans.reduce((sum, entry) => sum + (entry.deleted ?? 0), 0)
  };
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
