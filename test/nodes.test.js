"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const vm = require("node:vm");
const { createHarness } = require("./helpers/node-red-harness");

test("registers and exercises all outbox nodes", async () => {
  const harness = createHarness();
  assert.deepEqual(
    [...harness.types.keys()].sort(),
    [
      "durable-outbox-config",
      "outbox-claim",
      "outbox-control",
      "outbox-enqueue",
      "outbox-settle",
      "outbox-sink-config",
    ]
  );

  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
    maxJobMb: 1.5,
    maxDatabaseMb: 64,
    minFreeDiskMb: 32,
    deliveredRetentionMs: 3_600_000,
    cleanupIntervalMs: 5_000,
    cleanupBatchSize: 500,
    cleanupHighWatermarkPercent: 85,
    cleanupLowWatermarkPercent: 65,
    protectIngestion: true,
  });
  assert.equal(config.store.maxJobBytes, 1.5 * 1_048_576);
  assert.equal(config.store.maxDatabaseBytes, 64 * 1_048_576);
  assert.equal(config.store.minFreeDiskBytes, 32 * 1_048_576);
  assert.equal(config.store.deliveredRetentionMs, 3_600_000);
  assert.equal(config.cleanupIntervalMs, 5_000);
  assert.equal(config.store.cleanupBatchSize, 500);
  assert.equal(config.store.cleanupHighWatermark, 0.85);
  assert.equal(config.store.cleanupLowWatermark, 0.65);
  assert.equal(config.store.protectIngestion, true);
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
    maxAttempts: 8,
    retryUntilExpired: true,
    maxAgeMs: 604_800_000,
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    id: "enqueue",
    sink: "postgres-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    id: "claim",
    sink: "postgres-sink",
  });
  const settle = harness.instantiate("outbox-settle", {
    id: "settle",
    sink: "postgres-sink",
    outcome: "success",
  });
  const control = harness.instantiate("outbox-control", {
    id: "control",
    sink: "postgres-sink",
    action: "status",
  });
  assert.equal(enqueue.statuses.at(-1).text, "0 queued");

  await harness.input(enqueue, {
    payload: { deviceId: "sensor-1", value: 42 },
    outbox: { dedupeKey: "node-test" },
  });
  assert.equal(enqueue.sent[0].outbox.result.inserted, 1);
  assert.equal(enqueue.sent[0].outbox.result.queueDepth, 1);
  assert.equal(enqueue.statuses.at(-1).text, "1 queued");

  await harness.input(claim);
  assert.equal(enqueue.statuses.at(-1).text, "1 queued");
  const delivery = claim.sent[0];
  assert.deepEqual(delivery.payload, { deviceId: "sensor-1", value: 42 });
  assert.equal(delivery.outbox.attempts, 1);
  assert.equal(delivery.outbox.sink, "postgres");
  assert.equal(delivery.outbox.maxAttempts, 8);
  assert.equal(delivery.outbox.retryUntilExpired, true);

  await harness.input(settle, delivery);
  assert.equal(settle.sent[0][0].outbox.result.state, "delivered");
  assert.equal(settle.sent[0][1], null);
  assert.equal(enqueue.statuses.at(-1).text, "0 queued");

  await harness.input(control);
  assert.equal(control.sent[0].outbox.result.action, "status");
  assert.equal(control.sent[0].payload.jobs[0].state, "delivered");
  await harness.input(control, { outbox: { action: "check-integrity" } });
  assert.equal(control.sent[1].payload.ok, true);

  await harness.close(enqueue);
  await harness.close(claim);
  await harness.close(config);
});

test("durable outbox rejects inverted cleanup watermarks", () => {
  const harness = createHarness();
  assert.throws(
    () =>
      harness.instantiate("durable-outbox-config", {
        filename: ":memory:",
        cleanupHighWatermarkPercent: 70,
        cleanupLowWatermarkPercent: 80,
      }),
    /0 <= target < start < 100/
  );
});

test("default sink enables one full batch and age-bounded retry", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  const sink = harness.instantiate("outbox-sink-config", {
    id: "default-sink",
    outbox: "outbox",
    sinkKey: "postgres",
  });

  assert.equal(config.store.maxJobBytes, 1_048_576);
  assert.equal(sink.batchSize, 10);
  assert.equal(sink.maxInFlight, 10);
  assert.equal(sink.retryUntilExpired, true);
  assert.equal(sink.maxAgeMs, 86_400_000);

  await harness.close(config);
});

test("settle sends non-retryable failures to its dead-letter output", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "fieldkit-sink",
    outbox: "outbox",
    sinkKey: "fieldkit",
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "fieldkit-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    sink: "fieldkit-sink",
  });
  const settle = harness.instantiate("outbox-settle", {
    sink: "fieldkit-sink",
    outcome: "dead",
  });
  const control = harness.instantiate("outbox-control", {
    sink: "fieldkit-sink",
    action: "msg",
  });

  await harness.input(enqueue, {
    payload: { invalid: true },
  });
  await harness.input(claim);
  const message = claim.sent[0];
  if (!message.outbox) message.outbox = {};
  message.outbox.error = "HTTP 400";
  message.statusCode = 400;
  await harness.input(settle, message);

  assert.equal(settle.sent[0][0], null);
  assert.equal(settle.sent[0][1].outbox.result.state, "dead");
  assert.equal(config.store.listDeadLetters().length, 1);

  await harness.input(control, {
    outbox: {
      action: "requeue-one",
      deadLetterId: message.outbox.id,
    },
  });
  assert.equal(control.sent[0].payload.state, "pending");
  assert.equal(config.store.listDeadLetters().length, 0);

  await harness.close(claim);
  await harness.close(config);
});

test("claim emits a bounded batch and control exposes lifecycle actions", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
    leaseMs: 60_000,
    maxInFlight: 3,
    batchSize: 2,
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    sink: "postgres-sink",
  });
  const settle = harness.instantiate("outbox-settle", {
    sink: "postgres-sink",
    outcome: "success",
  });
  const control = harness.instantiate("outbox-control", {
    sink: "postgres-sink",
    action: "msg",
  });

  for (const value of [1, 2, 3]) {
    await harness.input(enqueue, {
      payload: { value },
      outbox: { dedupeKey: `node-batch-${value}` },
    });
  }
  await harness.input(claim);
  assert.equal(claim.sent.length, 2);
  assert.equal(
    claim.sent.every((message) => typeof message.outbox.leaseToken === "string"),
    true
  );
  await harness.input(settle, claim.sent[0]);
  await harness.input(control, {
    outbox: {
      action: "purge-delivered",
      olderThanMs: 0,
      limit: 1,
    },
  });
  assert.equal(control.sent[0].payload.purged, 1);

  await harness.input(control, { outbox: { action: "status" } });
  assert.equal(control.sent[1].outbox.result.sink, "postgres");
  assert.equal(typeof control.sent[1].payload.health.databaseBytes, "number");
  await harness.input(control, {
    outbox: {
      action: "maintenance",
      checkpoint: false,
    },
  });
  assert.equal(control.sent[2].payload.checkpoint, null);

  await harness.close(claim);
  await harness.close(config);
});

test("batch claim preserves leases and batch settle partitions outcomes", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
    maxInFlight: 3,
    batchSize: 3,
    maxAttempts: 3,
    retryUntilExpired: false,
    circuitBreakerThreshold: 2,
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    sink: "postgres-sink",
    outputMode: "batch",
  });
  const settle = harness.instantiate("outbox-settle", {
    sink: "postgres-sink",
    outcome: "retry",
  });

  for (const value of [1, 2, 3]) {
    await harness.input(enqueue, {
      payload: { value },
      outbox: {
        dedupeKey: `batch-${value}`,
        maxAttempts: value === 1 ? 1 : 3,
      },
    });
  }

  await harness.input(claim);
  assert.equal(claim.sent.length, 1);
  const batch = claim.sent[0];
  assert.deepEqual(
    batch.payload.map(({ value }) => value).sort(),
    [1, 2, 3]
  );
  assert.equal(batch.outbox.batch.length, 3);
  assert.equal(
    batch.outbox.batch.every(
      (lease, index) =>
        config.store.getJob(lease.id).payload.value ===
        batch.payload[index].value
    ),
    true
  );
  assert.equal(
    batch.outbox.batch.every(
      (lease) =>
        typeof lease.leaseToken === "string" &&
        !Object.prototype.hasOwnProperty.call(lease, "payload")
    ),
    true
  );

  batch.outbox.circuitFailure = true;
  batch.outbox.failureClass = "postgres-connectivity";
  batch.outbox.error = vm.runInNewContext(`({
    message: "PostgreSQL batch failed",
    payload: { cause: new Error("connection timed out") }
  })`);
  await harness.input(settle, batch);

  const [active, dead] = settle.sent[0];
  assert.equal(active.outbox.batch.length, 2);
  assert.equal(dead.outbox.batch.length, 1);
  assert.equal(active.outbox.result.retrying.length, 2);
  assert.equal(active.outbox.result.dead.length, 1);
  assert.equal(dead.outbox.result, active.outbox.result);
  assert.equal(
    config.store.getSinkControl("postgres").consecutiveFailures,
    1
  );
  assert.equal(
    JSON.parse(config.store.getJob(active.outbox.batch[0].id).lastError)
      .payload.cause.message,
    "connection timed out"
  );
  assert.equal(config.store.listDeadLetters().length, 1);

  await harness.close(enqueue);
  await harness.close(claim);
  await harness.close(config);
});

test("fixed-sink enqueue rejects a mismatched durable sink key", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });

  await assert.rejects(
    harness.input(enqueue, {
      payload: { value: 1 },
      outbox: { sink: "fieldkit" },
    }),
    /does not match configured sink/
  );
  await assert.rejects(
    harness.input(enqueue, {
      payload: { value: 1 },
      outbox: { id: "caller-controlled" },
    }),
    (error) => error.code === "OUTBOX_MANAGED_ID"
  );
  assert.equal(config.store.stats().health.queued, 0);
  await harness.close(config);
});

test("enqueue routes capacity rejection and rate-limits repeated errors", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
    protectIngestion: true,
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });
  const used = config.store.storageHealth().usedDatabaseBytes;
  config.store.maxDatabaseBytes = used + 512;

  await harness.input(enqueue, {
    payload: { bytes: "x".repeat(1_024) },
  });
  await harness.input(enqueue, {
    payload: { bytes: "y".repeat(1_024) },
  });

  assert.equal(enqueue.sent.length, 2);
  for (const output of enqueue.sent) {
    assert.equal(output[0], null);
    assert.equal(
      output[1].outbox.result.error.code,
      "OUTBOX_DATABASE_CAPACITY"
    );
    assert.equal(output[1].outbox.result.inserted, false);
  }
  assert.equal(enqueue.errors.length, 1);
  assert.match(enqueue.statuses.at(-1).text, /capacity blocked \(2\)/);
  assert.equal(config.store.countQueued("postgres"), 0);

  await harness.close(enqueue);
  await harness.close(config);
});

test("settle uses circuit policy from the selected sink config", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
    retryUntilExpired: true,
    maxAttempts: 1,
    circuitBreakerThreshold: 1,
    circuitBreakerCooldownMs: 60_000,
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    sink: "postgres-sink",
  });
  const settle = harness.instantiate("outbox-settle", {
    sink: "postgres-sink",
    outcome: "retry",
  });

  await harness.input(enqueue, { payload: { value: 1 } });
  await harness.input(claim);
  const message = claim.sent[0];
  message.outbox.failureClass = "postgres-connectivity";
  message.outbox.circuitFailure = true;
  message.outbox.error = "connection timed out";
  await harness.input(settle, message);

  const sinkControl = config.store.getSinkControl("postgres");
  assert.equal(settle.sent[0][0].outbox.result.state, "pending");
  assert.equal(sinkControl.consecutiveFailures, 1);
  assert.equal(sinkControl.pausedUntil > Date.now(), true);
  await harness.close(claim);
  await harness.close(config);
});

test("a disabled sink circuit breaker ignores circuit failures", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
    retryUntilExpired: true,
    circuitBreakerEnabled: false,
    circuitBreakerThreshold: 1,
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    sink: "postgres-sink",
  });
  const settle = harness.instantiate("outbox-settle", {
    sink: "postgres-sink",
    outcome: "retry",
  });

  await harness.input(enqueue, { payload: { value: 1 } });
  await harness.input(claim);
  const message = claim.sent[0];
  message.outbox.circuitFailure = true;
  message.outbox.error = "connection timed out";
  await harness.input(settle, message);

  const sinkControl = config.store.getSinkControl("postgres");
  assert.equal(sinkControl.consecutiveFailures, 0);
  assert.equal(sinkControl.pausedUntil, null);
  config.store.retryNow("postgres");
  await harness.input(claim);
  assert.equal(claim.sent.length, 2);

  await harness.close(enqueue);
  await harness.close(claim);
  await harness.close(config);
});

test("enqueue treats an array payload as one durable job", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  harness.instantiate("outbox-sink-config", {
    id: "postgres-sink",
    outbox: "outbox",
    sinkKey: "postgres",
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    sink: "postgres-sink",
  });
  const claim = harness.instantiate("outbox-claim", {
    sink: "postgres-sink",
  });

  await harness.input(enqueue, { payload: [{ value: 1 }, { value: 2 }] });
  assert.equal(enqueue.sent[0].outbox.result.inserted, 1);
  await harness.input(claim);
  assert.equal(claim.sent.length, 1);
  assert.deepEqual(claim.sent[0].payload, [{ value: 1 }, { value: 2 }]);

  await harness.close(claim);
  await harness.close(config);
});
