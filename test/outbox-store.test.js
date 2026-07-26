"use strict";

const assert = require("node:assert/strict");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const { DatabaseSync } = require("node:sqlite");
const { OutboxStore, stableStringify } = require("../lib/outbox-store");

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

test("migrates databases created by the original schema", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "durable-outbox-migration-"));
  const filename = join(directory, "outbox.sqlite");
  const legacy = new DatabaseSync(filename);
  legacy.exec(`
    CREATE TABLE outbox_jobs (
      id TEXT PRIMARY KEY, dedupe_key TEXT UNIQUE, sink TEXT,
      schema_version INTEGER, payload_json TEXT, state TEXT,
      attempts INTEGER, max_attempts INTEGER, max_age_ms INTEGER,
      base_delay_ms INTEGER, max_delay_ms INTEGER, available_at INTEGER,
      lease_until INTEGER, first_error TEXT, last_error TEXT,
      last_status INTEGER, created_at INTEGER, delivered_at INTEGER
    );
    CREATE TABLE dead_letter_jobs (
      id TEXT PRIMARY KEY, dedupe_key TEXT, sink TEXT,
      schema_version INTEGER, payload_json TEXT, attempts INTEGER,
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
    maxDatabaseBytes: 1,
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

  const health = store.stats().health;
  assert.equal(health.queued, 0);
  assert.equal(health.databaseSizeWarning, true);
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
