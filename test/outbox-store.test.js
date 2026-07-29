"use strict";

const assert = require("node:assert/strict");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const { DatabaseSync } = require("node:sqlite");
const {
  OutboxStore,
  serializeError,
  stableStringify,
} = require("../lib/outbox-store");

function withStore(t, options = {}) {
  const directory = mkdtempSync(join(tmpdir(), "durable-outbox-"));
  const store = new OutboxStore(join(directory, "outbox.sqlite"), options);
  t.after(() => {
    store.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return store;
}

function settle(store, id, outcome) {
  return store.settle(id, {
    ...outcome,
    leaseToken: store.getJob(id)?.leaseToken,
  });
}

test("stableStringify canonicalizes object keys", () => {
  assert.equal(
    stableStringify({ z: 1, a: { d: 4, b: 2 } }),
    '{"a":{"b":2,"d":4},"z":1}'
  );
});

test("error serialization tolerates nested cross-realm Error causes", (t) => {
  const wrapped = vm.runInNewContext(`({
    message: "PostgreSQL request failed",
    payload: {
      code: "ETIMEDOUT",
      cause: new Error("socket timeout")
    }
  })`);
  wrapped.payload.cause.cause = wrapped;

  const serialized = serializeError(wrapped);
  const details = JSON.parse(serialized);
  assert.equal(details.message, "PostgreSQL request failed");
  assert.equal(details.payload.code, "ETIMEDOUT");
  assert.equal(details.payload.cause.name, "Error");
  assert.equal(details.payload.cause.message, "socket timeout");
  assert.equal(details.payload.cause.cause, "[Circular]");

  const store = withStore(t);
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: { value: 1 },
  });
  store.claim({ sink: "postgres" });
  const result = settle(store, queued.id, {
    success: false,
    retryable: true,
    error: wrapped,
  });

  assert.equal(result.state, "pending");
  assert.deepEqual(JSON.parse(store.getJob(queued.id).lastError), details);
});

test("migrates databases created by the original schema", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "durable-outbox-migration-"));
  const filename = join(directory, "outbox.sqlite");
  const legacy = new DatabaseSync(filename);
  legacy.exec(`
    CREATE TABLE outbox_jobs (
      id TEXT PRIMARY KEY, dedupe_key TEXT UNIQUE, sink TEXT,
      payload_json TEXT, state TEXT,
      attempts INTEGER, max_attempts INTEGER, max_age_ms INTEGER,
      base_delay_ms INTEGER, max_delay_ms INTEGER, available_at INTEGER,
      lease_until INTEGER, first_error TEXT, last_error TEXT,
      last_status INTEGER, created_at INTEGER, delivered_at INTEGER
    );
    CREATE TABLE dead_letter_jobs (
      id TEXT PRIMARY KEY, dedupe_key TEXT, sink TEXT,
      payload_json TEXT, attempts INTEGER,
      max_attempts INTEGER, max_age_ms INTEGER, base_delay_ms INTEGER,
      max_delay_ms INTEGER, first_error TEXT, last_error TEXT,
      last_status INTEGER, created_at INTEGER, failed_at INTEGER
    );
  `);
  legacy.close();

  const store = new OutboxStore(filename);
  t.after(() => {
    store.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const activeColumns = store.db
    .prepare("PRAGMA table_info(outbox_jobs)")
    .all()
    .map((column) => column.name);
  const deadColumns = store.db
    .prepare("PRAGMA table_info(dead_letter_jobs)")
    .all()
    .map((column) => column.name);
  assert.equal(activeColumns.includes("retry_until_expired"), true);
  assert.equal(activeColumns.includes("last_failure_class"), true);
  assert.equal(activeColumns.includes("payload_encoding"), true);
  assert.equal(activeColumns.includes("lease_token"), true);
  assert.equal(deadColumns.includes("retry_until_expired"), true);
  assert.equal(deadColumns.includes("last_failure_class"), true);
  assert.equal(deadColumns.includes("payload_encoding"), true);
});

test("serialization preserves Node-RED values and rejects unsafe payloads", (t) => {
  const store = withStore(t);
  const timestamp = new Date("2026-07-26T12:34:56.000Z");
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: {
      timestamp,
      bytes: Buffer.from([0, 1, 2, 255]),
      omitted: undefined,
      array: [1, undefined, 3],
    },
  });
  const claimed = store.claim({ sink: "postgres" });
  assert.equal(claimed.id, queued.id);
  assert.equal(claimed.payload.timestamp instanceof Date, true);
  assert.equal(claimed.payload.timestamp.toISOString(), timestamp.toISOString());
  assert.deepEqual(claimed.payload.bytes, Buffer.from([0, 1, 2, 255]));
  assert.equal("omitted" in claimed.payload, false);
  assert.deepEqual(claimed.payload.array, [1, null, 3]);

  const circular = {};
  circular.self = circular;
  assert.throws(
    () => store.enqueue({ sink: "postgres", payload: circular }),
    /Circular reference/
  );
  assert.throws(
    () => store.enqueue({ sink: "postgres", payload: { work() {} } }),
    /Unsupported function/
  );

  const functionNodePayload = vm.runInNewContext(
    "({ device_id: 'sensor-1', value: 42 })"
  );
  const [crossRealm] = store.enqueue({
    sink: "postgres",
    dedupeKey: "cross-realm",
    payload: functionNodePayload,
  });
  assert.deepEqual(store.getJob(crossRealm.id).payload, {
    device_id: "sensor-1",
    value: 42,
  });
});

test("enqueue is atomic and deduplicates equivalent jobs", (t) => {
  const store = withStore(t, { now: () => 1_000 });
  assert.throws(
    () =>
      store.enqueue([
        { sink: "postgres", payload: { value: 1 } },
        { sink: "postgres" },
      ]),
    /requires a payload/
  );
  const emptyStats = store.stats();
  assert.deepEqual(emptyStats.jobs, []);
  assert.equal(emptyStats.deadLetters, 0);
  assert.deepEqual(emptyStats.controls, []);
  assert.equal(emptyStats.health.queued, 0);
  assert.equal(store.countQueued("postgres"), 0);

  const first = store.enqueue({
    sink: "postgres",
    payload: { b: 2, a: 1 },
  })[0];
  const duplicate = store.enqueue({
    sink: "postgres",
    payload: { a: 1, b: 2 },
  })[0];

  assert.equal(first.inserted, true);
  assert.equal(duplicate.inserted, false);
  assert.equal(duplicate.id, first.id);
  assert.equal(store.countQueued("postgres"), 1);
});

test("claim enforces concurrency and recovers expired leases", (t) => {
  let now = 10_000;
  const store = withStore(t, { now: () => now });
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: { value: 7 },
  });

  const first = store.claim({
    sink: "postgres",
    leaseMs: 1_000,
    maxInFlight: 1,
  });
  assert.equal(first.id, queued.id);
  assert.equal(first.attempts, 1);
  assert.equal(
    store.claim({ sink: "postgres", leaseMs: 1_000, maxInFlight: 1 }),
    null
  );

  now = 11_001;
  const recovered = store.claim({
    sink: "postgres",
    leaseMs: 1_000,
    maxInFlight: 1,
  });
  assert.equal(recovered.id, queued.id);
  assert.equal(recovered.attempts, 2);
});

test("lease tokens fence stale workers after a reclaim", (t) => {
  let now = 12_000;
  const store = withStore(t, { now: () => now });
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: { value: 7 },
  });
  const stale = store.claim({ sink: "postgres", leaseMs: 100 });
  now += 101;
  const current = store.claim({ sink: "postgres", leaseMs: 100 });

  assert.notEqual(current.leaseToken, stale.leaseToken);
  assert.deepEqual(
    store.settle(queued.id, {
      leaseToken: stale.leaseToken,
      success: true,
    }),
    { id: queued.id, state: "stale_lease" }
  );
  assert.equal(store.getJob(queued.id).leaseToken, current.leaseToken);
  assert.equal(
    store.settle(queued.id, {
      leaseToken: current.leaseToken,
      success: true,
    }).state,
    "delivered"
  );
});

test("batch claims fill only remaining in-flight capacity", (t) => {
  const store = withStore(t);
  store.enqueue(
    Array.from({ length: 8 }, (_, index) => ({
      sink: "postgres",
      dedupeKey: `batch-${index}`,
      payload: { index },
    }))
  );
  const first = store.claimBatch({
    sink: "postgres",
    maxInFlight: 5,
    batchSize: 3,
  });
  const second = store.claimBatch({
    sink: "postgres",
    maxInFlight: 5,
    batchSize: 10,
  });
  const full = store.claimBatch({
    sink: "postgres",
    maxInFlight: 5,
    batchSize: 10,
  });
  assert.equal(first.length, 3);
  assert.equal(second.length, 2);
  assert.equal(full.length, 0);
  assert.equal(new Set([...first, ...second].map((job) => job.id)).size, 5);
});

test("batch settlement is atomic", (t) => {
  const store = withStore(t);
  store.enqueue([
    { sink: "postgres", dedupeKey: "settle-batch-1", payload: { value: 1 } },
    { sink: "postgres", dedupeKey: "settle-batch-2", payload: { value: 2 } },
  ]);
  const jobs = store.claimBatch({
    sink: "postgres",
    maxInFlight: 2,
    batchSize: 2,
  });

  assert.throws(
    () =>
      store.settleBatch([
        {
          id: jobs[0].id,
          outcome: { leaseToken: jobs[0].leaseToken, success: true },
        },
        {
          id: "missing-job",
          outcome: { leaseToken: jobs[1].leaseToken, success: true },
        },
      ]),
    /does not exist/
  );
  assert.equal(store.getJob(jobs[0].id).state, "leased");

  const results = store.settleBatch(
    jobs.map((job) => ({
      id: job.id,
      outcome: { leaseToken: job.leaseToken, success: true },
    }))
  );
  assert.deepEqual(
    results.map((result) => result.state),
    ["delivered", "delivered"]
  );
});

test("repeatedly abandoned leases dead-letter at the attempt bound", (t) => {
  let now = 15_000;
  const store = withStore(t, { now: () => now });
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: { value: 9 },
    maxAttempts: 2,
  });

  store.claim({ sink: "postgres", leaseMs: 100 });
  now += 101;
  store.claim({ sink: "postgres", leaseMs: 100 });
  now += 101;

  assert.equal(store.claim({ sink: "postgres", leaseMs: 100 }), null);
  assert.equal(store.getJob(queued.id), null);
  const [dead] = store.listDeadLetters();
  assert.equal(dead.id, queued.id);
  assert.match(dead.lastError, /maximum delivery attempts/);
});

test("retry delay is bounded exponential backoff with jitter", (t) => {
  let now = 20_000;
  const store = withStore(t, { now: () => now, random: () => 0.5 });
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: { value: 8 },
    baseDelayMs: 2_000,
    maxDelayMs: 3_000,
  });

  store.claim({ sink: "postgres" });
  const first = settle(store, queued.id, {
    success: false,
    retryable: true,
    error: new Error("offline"),
  });
  assert.equal(first.delayMs, 1_000);

  now = first.availableAt;
  store.claim({ sink: "postgres" });
  const second = settle(store, queued.id, {
    success: false,
    retryable: true,
    error: "still offline",
  });
  assert.equal(second.delayMs, 1_500);
  assert.equal(store.getJob(queued.id).firstError.includes("offline"), true);
});

test("success is retained and retry exhaustion moves atomically to dead letter", (t) => {
  let now = 30_000;
  const store = withStore(t, { now: () => now, random: () => 0 });
  const [successful, doomed] = store.enqueue([
    {
      sink: "postgres",
      dedupeKey: "successful",
      payload: { value: 1 },
    },
    {
      sink: "fieldkit",
      dedupeKey: "doomed",
      payload: { value: 2 },
      maxAttempts: 2,
    },
  ]);

  store.claim({ sink: "postgres" });
  assert.equal(
    settle(store, successful.id, { success: true }).state,
    "delivered"
  );
  assert.equal(store.getJob(successful.id).state, "delivered");

  store.claim({ sink: "fieldkit" });
  settle(store, doomed.id, {
    success: false,
    retryable: true,
    error: "failure one",
  });
  store.claim({ sink: "fieldkit" });
  const dead = settle(store, doomed.id, {
    success: false,
    retryable: true,
    error: "failure two",
    status: 503,
  });
  assert.equal(dead.state, "dead");
  assert.equal(dead.reason, "attempts-exhausted");
  assert.equal(store.getJob(doomed.id), null);
  assert.equal(store.listDeadLetters()[0].lastStatus, 503);

  now += 1;
  assert.equal(store.requeueDeadLetter(doomed.id).state, "pending");
  assert.equal(store.getJob(doomed.id).attempts, 0);
  assert.equal(store.listDeadLetters().length, 0);
});

test("non-retryable errors dead-letter immediately", (t) => {
  const store = withStore(t);
  const [queued] = store.enqueue({
    sink: "fieldkit",
    payload: { malformed: true },
  });
  store.claim({ sink: "fieldkit" });
  const result = settle(store, queued.id, {
    success: false,
    retryable: false,
    error: "HTTP 400",
    status: 400,
  });
  assert.equal(result.state, "dead");
  assert.equal(result.reason, "non-retryable");
});

test("infrastructure jobs retry past attempt limit and open a circuit", (t) => {
  let now = 40_000;
  const store = withStore(t, { now: () => now, random: () => 0 });
  const [queued] = store.enqueue({
    sink: "postgres",
    payload: { value: 10 },
    maxAttempts: 1,
    retryUntilExpired: true,
    maxAgeMs: 7 * 86_400_000,
  });

  store.claim({ sink: "postgres" });
  const retry = settle(store, queued.id, {
    success: false,
    retryable: true,
    failureClass: "infrastructure",
    error: { code: "ETIMEDOUT" },
    circuitBreakerThreshold: 1,
    circuitBreakerCooldownMs: 30_000,
  });
  assert.equal(retry.state, "pending");
  assert.equal(retry.circuit.pausedUntil, now + 30_000);
  assert.equal(store.claim({ sink: "postgres" }), null);

  const resumed = store.resumeSink("postgres", { retryNow: true });
  assert.equal(resumed.released, 1);
  const secondAttempt = store.claim({ sink: "postgres" });
  assert.equal(secondAttempt.attempts, 2);
  assert.equal(settle(store, queued.id, { success: true }).state, "delivered");
  assert.equal(store.getSinkControl("postgres").consecutiveFailures, 0);
});

test("a half-open circuit leases a single probe until it recovers", (t) => {
  let now = 100_000;
  const store = withStore(t, { now: () => now, random: () => 0 });
  for (let value = 0; value < 3; value += 1) {
    store.enqueue({
      sink: "postgres",
      payload: { value },
      retryUntilExpired: true,
      maxAgeMs: 7 * 86_400_000,
    });
  }
  const claimAll = () =>
    store.claimBatch({ sink: "postgres", maxInFlight: 5, batchSize: 10 });

  const [opener] = store.claimBatch({
    sink: "postgres",
    maxInFlight: 1,
    batchSize: 1,
  });
  settle(store, opener.id, {
    success: false,
    retryable: true,
    circuitFailure: true,
    failureClass: "infrastructure",
    error: { code: "ETIMEDOUT" },
    circuitBreakerThreshold: 1,
    circuitBreakerCooldownMs: 30_000,
  });
  assert.equal(store.getSinkControl("postgres").pausedUntil, now + 30_000);

  // Still cooling down: claims are refused entirely.
  assert.deepEqual(claimAll(), []);

  // Cooldown elapsed → half-open: a single probe despite ample capacity.
  now += 30_000;
  const probe = claimAll();
  assert.equal(probe.length, 1);

  // A successful probe closes the circuit and restores full batching.
  settle(store, probe[0].id, { success: true });
  assert.equal(store.getSinkControl("postgres").pausedUntil, null);
  assert.equal(claimAll().length, 2);
});

test("circuit accounting is independent of retryability and failure class", (t) => {
  const store = withStore(t, { random: () => 0 });
  function fail(value, outcome) {
    store.enqueue({
      sink: "postgres",
      payload: { value },
    });
    const claimed = store.claim({ sink: "postgres" });
    assert.ok(claimed);
    return settle(store, claimed.id, {
      success: false,
      error: `failure ${value}`,
      circuitBreakerThreshold: 2,
      ...outcome,
    });
  }

  const dead = fail(1, {
    retryable: false,
    circuitFailure: true,
    failureClass: "postgres-connectivity",
  });
  assert.equal(dead.state, "dead");
  assert.equal(store.getSinkControl("postgres").consecutiveFailures, 1);

  fail(2, {
    retryable: true,
    circuitFailure: false,
    failureClass: "data",
  });
  assert.equal(store.getSinkControl("postgres").consecutiveFailures, 1);

  const result = fail(3, {
    retryable: true,
    circuitFailure: true,
    failureClass: "custom-label",
  });
  assert.equal(result.circuit.consecutiveFailures, 2);
  assert.equal(result.circuit.pausedUntil != null, true);
});

test("circuit failures are clamped at the configured threshold", (t) => {
  const store = withStore(t, { now: () => 5_000 });
  let control;
  for (let i = 0; i < 10; i += 1) {
    control = store.recordSinkFailure("postgres", {
      circuitFailure: true,
      threshold: 3,
      cooldownMs: 1_000,
      errorText: "boom",
    });
  }
  assert.equal(control.consecutiveFailures, 3);
  assert.equal(control.pausedUntil, 6_000);
  store.recordSinkSuccess("postgres");
  assert.equal(store.getSinkControl("postgres").consecutiveFailures, 0);
});

test("maintenance expires exhausted jobs even while a sink is paused", (t) => {
  let now = 1_000_000;
  const store = withStore(t, { now: () => now });
  const [job] = store.enqueue({
    sink: "postgres",
    payload: { value: 1 },
    maxAgeMs: 1_000,
  });
  store.pauseSink("postgres");
  now += 5_000;

  // A paused sink neither claims nor sweeps.
  assert.deepEqual(store.claimBatch({ sink: "postgres" }), []);
  assert.equal(store.stats().deadLetters, 0);

  // Maintenance expires the aged job regardless of the pause.
  const result = store.maintenance({ checkpoint: false });
  assert.equal(result.sweep.swept, 1);
  assert.equal(store.stats().deadLetters, 1);
  assert.equal(store.listDeadLetters({ sink: "postgres" })[0].id, job.id);
});

test("bulk dead-letter recovery filters by sink and failure class", (t) => {
  const store = withStore(t);
  const jobs = store.enqueue([
    {
      sink: "postgres",
      dedupeKey: "postgres-dead",
      payload: { value: 11 },
    },
    {
      sink: "fieldkit",
      dedupeKey: "fieldkit-dead",
      payload: { value: 12 },
    },
  ]);
  for (const job of jobs) {
    const claimed = store.claim({
      sink: job.dedupeKey.startsWith("postgres") ? "postgres" : "fieldkit",
    });
    settle(store, claimed.id, {
      success: false,
      retryable: false,
      failureClass: "infrastructure",
      error: "expired outage",
    });
  }

  const result = store.requeueDeadLetters({
    sink: "postgres",
    failureClass: "infrastructure",
  });
  assert.equal(result.requeued, 1);
  assert.equal(store.getJob(jobs[0].id).state, "pending");
  assert.equal(store.getJob(jobs[1].id), null);
  assert.equal(store.listDeadLetters().length, 1);
});

test("dead-letter filters are applied before the result limit", (t) => {
  let now = 50_000;
  const store = withStore(t, { now: () => now });
  for (const [index, sink] of ["postgres", "fieldkit", "fieldkit"].entries()) {
    const [job] = store.enqueue({
      sink,
      dedupeKey: `filtered-${index}`,
      payload: { index },
    });
    const claimed = store.claim({ sink });
    settle(store, job.id, {
      success: false,
      retryable: false,
      failureClass: sink === "postgres" ? "infrastructure" : "data",
      error: "failed",
    });
    now += 1;
  }

  const rows = store.listDeadLetters({
    sink: "postgres",
    failureClass: "infrastructure",
    limit: 1,
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].sink, "postgres");
});

test("retention, deletion, health, and capacity controls are bounded", (t) => {
  let now = 60_000;
  const store = withStore(t, {
    now: () => now,
    maxQueuedJobs: 2,
    maxJobBytes: 256,
    maxEnqueueBatch: 2,
  });
  const jobs = store.enqueue([
    { sink: "postgres", dedupeKey: "retained-1", payload: { value: 1 } },
    { sink: "postgres", dedupeKey: "retained-2", payload: { value: 2 } },
  ]);
  assert.throws(
    () =>
      store.enqueue({
        sink: "postgres",
        dedupeKey: "over-capacity",
        payload: { value: 3 },
      }),
    /queue exceeds/
  );

  const first = store.claim({ sink: "postgres", maxInFlight: 2 });
  settle(store, first.id, { success: true });
  now += 100;
  const second = store.claim({ sink: "postgres", maxInFlight: 2 });
  settle(store, second.id, {
    success: false,
    retryable: false,
    failureClass: "data",
    error: "bad row",
  });

  store.maxDatabaseBytes = 1;
  const health = store.stats().health;
  assert.equal(health.queued, 0);
  assert.equal(health.databaseSizeWarning, true);
  store.maxDatabaseBytes = 1_073_741_824;
  assert.equal(
    store.purgeDelivered({ olderThanMs: 50, limit: 1 }).purged,
    1
  );
  assert.equal(
    store.deleteDeadLetters({
      sink: "postgres",
      failureClass: "data",
      limit: 1,
    }).deleted,
    1
  );
  assert.equal(store.getJob(jobs[0].id), null);
  assert.equal(store.listDeadLetters().length, 0);
  assert.throws(
    () =>
      store.enqueue({
        sink: "postgres",
        payload: { text: "x".repeat(1_000) },
      }),
    /payload exceeds/
  );
  assert.throws(
    () =>
      store.enqueue([
        { sink: "postgres", payload: 1 },
        { sink: "postgres", payload: 2 },
        { sink: "postgres", payload: 3 },
      ]),
    /batch exceeds/
  );
});

test("automatic cleanup bounds retention work and reports progress", (t) => {
  let now = 100_000;
  const store = withStore(t, {
    now: () => now,
    deliveredRetentionMs: 100,
    cleanupBatchSize: 2,
  });

  for (let index = 0; index < 3; index += 1) {
    const [job] = store.enqueue({
      sink: "postgres",
      dedupeKey: `cleanup-${index}`,
      payload: { index },
    });
    store.claim({ sink: "postgres" });
    settle(store, job.id, { success: true });
  }
  now += 101;

  const first = store.cleanupDelivered();
  assert.equal(first.purged, 2);
  assert.equal(first.expiredPurged, 2);
  assert.equal(first.pressureEvicted, 0);
  assert.equal(first.needsMore, true);

  const second = store.cleanupDelivered();
  assert.equal(second.purged, 1);
  assert.equal(second.needsMore, false);
  const health = store.stats().health;
  assert.equal(health.delivered, 0);
  assert.equal(health.cleanup.totalExpiredPurged, 3);
  assert.equal(health.cleanup.totalPressureEvicted, 0);
});

test("storage pressure evicts only delivered history toward the low watermark", (t) => {
  let now = 200_000;
  const store = withStore(t, {
    now: () => now,
    deliveredRetentionMs: 86_400_000,
    cleanupBatchSize: 2,
    cleanupHighWatermark: 0.5,
    cleanupLowWatermark: 0.25,
  });

  for (let index = 0; index < 3; index += 1) {
    const [job] = store.enqueue({
      sink: "postgres",
      dedupeKey: `pressure-delivered-${index}`,
      payload: { bytes: "x".repeat(4_096), index },
    });
    store.claim({ sink: "postgres" });
    settle(store, job.id, { success: true });
    now += 1;
  }
  const [failed] = store.enqueue({
    sink: "postgres",
    dedupeKey: "pressure-dead-letter",
    payload: { keep: "dead" },
  });
  store.claim({ sink: "postgres", maxInFlight: 2 });
  settle(store, failed.id, {
    success: false,
    retryable: false,
    failureClass: "data",
    error: "keep this dead letter",
  });
  store.enqueue({
    sink: "postgres",
    dedupeKey: "pressure-pending",
    payload: { keep: true },
  });
  const used = store.storageHealth().usedDatabaseBytes;
  store.maxDatabaseBytes = used;

  const cleanup = store.cleanupDelivered();
  assert.equal(cleanup.purged, 2);
  assert.equal(cleanup.expiredPurged, 0);
  assert.equal(cleanup.pressureEvicted, 2);
  assert.equal(store.countQueued("postgres"), 1);
  assert.equal(store.listDeadLetters().length, 1);
  assert.equal(store.getJob(
    store.db
      .prepare("SELECT id FROM outbox_jobs WHERE dedupe_key = ?")
      .get("pressure-pending").id
  ).state, "pending");
});

test("enqueue reclaims delivered history before returning database capacity", (t) => {
  const store = withStore(t, {
    cleanupBatchSize: 10,
    deliveredRetentionMs: 86_400_000,
  });
  for (let index = 0; index < 6; index += 1) {
    const [job] = store.enqueue({
      sink: "postgres",
      dedupeKey: `admission-history-${index}`,
      payload: { bytes: "x".repeat(8_192), index },
    });
    store.claim({ sink: "postgres" });
    settle(store, job.id, { success: true });
  }

  const used = store.storageHealth().usedDatabaseBytes;
  store.maxDatabaseBytes = used + 512;
  const result = store.enqueue({
    sink: "postgres",
    dedupeKey: "admitted-after-cleanup",
    payload: { bytes: "y".repeat(8_192) },
  });

  assert.equal(result[0].inserted, true);
  assert.equal(result.cleanup.attempted, true);
  assert.ok(result.cleanup.purged > 0);
  assert.equal(store.countQueued("postgres"), 1);
  assert.ok(
    store.stats().health.delivered < 6,
    "admission should sacrifice delivered history, not the new active job"
  );
});

test("a duplicate remains admissible without evicting its delivered record", (t) => {
  const store = withStore(t);
  const [job] = store.enqueue({
    sink: "postgres",
    dedupeKey: "delivered-duplicate-at-capacity",
    payload: { value: 1 },
  });
  store.claim({ sink: "postgres" });
  settle(store, job.id, { success: true });
  store.maxDatabaseBytes = 1;

  const duplicate = store.enqueue({
    sink: "postgres",
    dedupeKey: "delivered-duplicate-at-capacity",
    payload: { value: 1 },
  });

  assert.equal(duplicate[0].inserted, false);
  assert.equal(duplicate[0].state, "delivered");
  assert.equal(duplicate.cleanup.attempted, false);
  assert.equal(store.getJob(job.id).state, "delivered");
});

test("enqueue manages job ids and reports distinct admission failures", (t) => {
  const store = withStore(t, {
    diskFreeBytes: 1_024,
    minFreeDiskBytes: 2_048,
  });

  assert.throws(
    () =>
      store.enqueue({
        id: "caller-controlled",
        sink: "postgres",
        payload: { value: 1 },
      }),
    (error) => error.code === "OUTBOX_MANAGED_ID"
  );
  assert.throws(
    () =>
      store.enqueue({
        sink: "postgres",
        payload: { value: 1 },
      }),
    (error) => error.code === "OUTBOX_DISK_HEADROOM"
  );

  store.minFreeDiskBytes = 0;
  const used = store.storageHealth().usedDatabaseBytes;
  store.maxDatabaseBytes = used + 512;
  assert.throws(
    () =>
      store.enqueue({
        sink: "postgres",
        payload: { value: "x".repeat(1_024) },
      }),
    (error) => error.code === "OUTBOX_DATABASE_CAPACITY"
  );
  assert.equal(store.countQueued("postgres"), 0);
});

test("claim quarantines invalid payloads without blocking healthy jobs", (t) => {
  let now = 70_000;
  const store = withStore(t, { now: () => now });
  const [corrupt] = store.enqueue({
    sink: "postgres",
    dedupeKey: "corrupt",
    payload: { value: "bad" },
  });
  now += 1;
  const [healthy] = store.enqueue({
    sink: "postgres",
    dedupeKey: "healthy",
    payload: { value: "good" },
  });
  store.db
    .prepare("UPDATE outbox_jobs SET payload_json = ? WHERE id = ?")
    .run("{not-json", corrupt.id);

  const jobs = store.claimBatch({
    sink: "postgres",
    batchSize: 2,
    maxInFlight: 2,
  });
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].id, healthy.id);
  assert.equal(jobs.quarantined, 1);
  assert.equal(store.countQueued("postgres"), 1);

  const dead = store.listDeadLetters();
  assert.equal(dead.length, 1);
  assert.equal(dead[0].lastFailureClass, "outbox-corruption");
  assert.equal(dead[0].payload, null);
  assert.equal(dead[0].payloadRaw, "{not-json");
  assert.equal(dead[0].replayable, false);
  assert.equal(dead[0].payloadDecodeError.code, "OUTBOX_INVALID_PAYLOAD");
  assert.throws(
    () => store.requeueDeadLetter(corrupt.id),
    (error) => error.code === "OUTBOX_CORRUPT_REPLAY"
  );
  assert.deepEqual(
    store.requeueDeadLetters({ sink: "postgres" }),
    {
      requeued: 0,
      ids: [],
      sink: "postgres",
      failureClass: null,
      corruptSkipped: 1,
    }
  );
});

test("unknown payload encodings pause rather than quarantine a sink", (t) => {
  const store = withStore(t);
  const [job] = store.enqueue({
    sink: "postgres",
    payload: { value: 1 },
  });
  store.db
    .prepare("UPDATE outbox_jobs SET payload_encoding = ? WHERE id = ?")
    .run("future-v3", job.id);

  assert.throws(
    () => store.claim({ sink: "postgres" }),
    (error) =>
      error.code === "OUTBOX_UNSUPPORTED_ENCODING" &&
      error.sinkPaused === true
  );
  assert.equal(store.getSinkControl("postgres").manuallyPaused, true);
  assert.equal(store.listDeadLetters().length, 0);
  const row = store.db
    .prepare("SELECT state, attempts FROM outbox_jobs WHERE id = ?")
    .get(job.id);
  assert.equal(row.state, "pending");
  assert.equal(row.attempts, 0);
});

test("claim bounds corrupt-payload quarantine work per poll", (t) => {
  const store = withStore(t);
  const jobs = store.enqueue(
    Array.from({ length: 11 }, (_, index) => ({
      sink: "postgres",
      dedupeKey: `corrupt-bound-${index}`,
      payload: { index },
    }))
  );
  const corrupt = store.db.prepare(
    "UPDATE outbox_jobs SET payload_json = ? WHERE id = ?"
  );
  for (const job of jobs) corrupt.run("{bad", job.id);

  const first = store.claimBatch({
    sink: "postgres",
    batchSize: 20,
    maxInFlight: 20,
  });
  assert.equal(first.length, 0);
  assert.equal(first.quarantined, 10);
  assert.equal(store.countQueued("postgres"), 1);

  const second = store.claimBatch({
    sink: "postgres",
    batchSize: 20,
    maxInFlight: 20,
  });
  assert.equal(second.quarantined, 1);
  assert.equal(store.countQueued("postgres"), 0);
  assert.equal(store.listDeadLetters({ limit: 20 }).length, 11);
});

test("integrity checks detect drift and startup reconciles counters and triggers", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "durable-outbox-integrity-"));
  const filename = join(directory, "outbox.sqlite");
  let store = new OutboxStore(filename);
  t.after(() => {
    store?.close();
    rmSync(directory, { recursive: true, force: true });
  });
  store.enqueue({ sink: "postgres", payload: { value: 1 } });
  assert.equal(store.checkIntegrity().ok, true);

  store.db
    .prepare("UPDATE outbox_queue_counts SET queued = 0 WHERE sink = ?")
    .run("postgres");
  store.db.exec("DROP TRIGGER outbox_queue_count_insert");
  const drift = store.checkIntegrity({ sqlite: false });
  assert.equal(drift.ok, false);
  assert.equal(drift.countersMatch, false);
  assert.deepEqual(drift.missingTriggers, ["outbox_queue_count_insert"]);

  store.close();
  store = new OutboxStore(filename);
  assert.equal(store.startupIntegrity.ok, true);
  assert.equal(store.countQueued("postgres"), 1);
  assert.equal(store.checkIntegrity().ok, true);
});
