/**
 * Small dependency-free metrics registry. Counters are monotonic; gauges are
 * current values; histograms retain aggregate count/sum/min/max rather than
 * unbounded raw samples.
 */
export class MetricsRegistry {
  #counters = new Map();
  #gauges = new Map();
  #histograms = new Map();

  increment(name, value = 1, labels = {}) {
    assertMetricName(name);
    assertFiniteNonNegative("counter value", value);
    const key = metricKey(name, labels);
    const record = this.#counters.get(key) ?? { name, labels: normalizeLabels(labels), value: 0 };
    record.value += value;
    this.#counters.set(key, record);
    return record.value;
  }

  setGauge(name, value, labels = {}) {
    assertMetricName(name);
    assertFinite("gauge value", value);
    const key = metricKey(name, labels);
    const record = { name, labels: normalizeLabels(labels), value };
    this.#gauges.set(key, record);
    return value;
  }

  observe(name, value, labels = {}) {
    assertMetricName(name);
    assertFiniteNonNegative("observation", value);
    const key = metricKey(name, labels);
    const record = this.#histograms.get(key) ?? { name, labels: normalizeLabels(labels), count: 0, sum: 0, min: value, max: value };
    record.count += 1;
    record.sum += value;
    record.min = Math.min(record.min, value);
    record.max = Math.max(record.max, value);
    this.#histograms.set(key, record);
    return freezeClone(record);
  }

  snapshot() {
    return freezeClone({
      counters: sortMetrics([...this.#counters.values()]),
      gauges: sortMetrics([...this.#gauges.values()]),
      histograms: sortMetrics([...this.#histograms.values()])
    });
  }
}

function metricKey(name, labels) {
  return `${name}\u0000${JSON.stringify(normalizeLabels(labels))}`;
}

function normalizeLabels(labels) {
  if (labels === null || typeof labels !== "object" || Array.isArray(labels)) throw new TypeError("metric labels must be an object");
  const normalized = {};
  for (const key of Object.keys(labels).sort()) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/.test(key)) throw new TypeError(`metric label ${key} has an invalid name`);
    const value = labels[key];
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") throw new TypeError(`metric label ${key} must be scalar`);
    normalized[key] = String(value);
  }
  return normalized;
}

function sortMetrics(metrics) {
  return metrics.sort((left, right) => metricKey(left.name, left.labels).localeCompare(metricKey(right.name, right.labels)));
}

function assertMetricName(name) {
  if (typeof name !== "string" || !/^[a-zA-Z_][a-zA-Z0-9_:]{0,127}$/.test(name)) throw new TypeError("metric name has an invalid format");
}

function assertFinite(name, value) {
  if (typeof value !== "number" || !Number.isFinite(value)) throw new TypeError(`${name} must be finite`);
}

function assertFiniteNonNegative(name, value) {
  assertFinite(name, value);
  if (value < 0) throw new TypeError(`${name} must not be negative`);
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
