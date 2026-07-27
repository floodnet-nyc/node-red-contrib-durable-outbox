"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const test = require("node:test");
const registerNodes = require("../nodes/outbox");

function createHarness() {
  const types = new Map();
  const instances = new Map();
  let idCounter = 0;

  const RED = {
    nodes: {
      registerType(name, constructor) {
        types.set(name, constructor);
      },
      createNode(node, config) {
        Object.setPrototypeOf(node, EventEmitter.prototype);
        EventEmitter.call(node);
        node.id = config.id || `node-${++idCounter}`;
        node.sent = [];
        node.statuses = [];
        node.errors = [];
        node.send = (message) => node.sent.push(message);
        node.status = (status) => node.statuses.push(status);
        node.error = (error) => node.errors.push(error);
        instances.set(node.id, node);
      },
      getNode(id) {
        return instances.get(id);
      },
    },
    util: {
      getMessageProperty(msg, property) {
        return property.split(".").reduce((value, key) => value?.[key], msg);
      },
      generateId() {
        return `message-${++idCounter}`;
      },
    },
  };

  registerNodes(RED);

  function instantiate(type, config = {}) {
    const Constructor = types.get(type);
    assert.ok(Constructor, `Node type ${type} was registered`);
    return new Constructor(config);
  }

  function input(node, msg = {}) {
    return new Promise((resolve, reject) => {
      node.emit(
        "input",
        msg,
        (message) => node.sent.push(message),
        (error) => (error ? reject(error) : resolve())
      );
    });
  }

  function close(node) {
    return new Promise((resolve, reject) => {
      node.emit("close", false, (error) => (error ? reject(error) : resolve()));
    });
  }

  return { instantiate, input, close, types };
}

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
  });
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

  await harness.input(enqueue, {
    payload: { deviceId: "sensor-1", value: 42 },
    outboxJob: { dedupeKey: "node-test" },
  });
  assert.equal(enqueue.sent[0].outboxEnqueue.inserted, 1);

  await harness.input(claim);
  const delivery = claim.sent[0];
  assert.deepEqual(delivery.payload, { deviceId: "sensor-1", value: 42 });
  assert.equal(delivery.outbox.attempts, 1);
  assert.equal(delivery.outbox.sink, "postgres");
  assert.equal(delivery.outbox.maxAttempts, 8);
  assert.equal(delivery.outbox.retryUntilExpired, true);

  await harness.input(settle, delivery);
  assert.equal(settle.sent[0][0].outboxSettlement.state, "delivered");
  assert.equal(settle.sent[0][1], null);

  await harness.input(control);
  assert.equal(control.sent[0].outboxControl.action, "status");
  assert.equal(control.sent[0].payload.jobs[0].state, "delivered");

  await harness.close(claim);
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
  message.outboxError = "HTTP 400";
  message.statusCode = 400;
  await harness.input(settle, message);

  assert.equal(settle.sent[0][0], null);
  assert.equal(settle.sent[0][1].outboxSettlement.state, "dead");
  assert.equal(config.store.listDeadLetters().length, 1);

  await harness.input(control, {
    outboxAction: "requeue-one",
    deadLetterId: message.outbox.id,
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
      outboxJob: { dedupeKey: `node-batch-${value}` },
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
    outboxAction: "purge-delivered",
    olderThanMs: 0,
    limit: 1,
  });
  assert.equal(control.sent[0].payload.purged, 1);

  await harness.input(control, { outboxAction: "status" });
  assert.equal(control.sent[1].outboxControl.sink, "postgres");
  assert.equal(typeof control.sent[1].payload.health.databaseBytes, "number");
  await harness.input(control, {
    outboxAction: "maintenance",
    checkpoint: false,
  });
  assert.equal(control.sent[2].payload.checkpoint, null);

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
      outboxJob: { sink: "fieldkit" },
    }),
    /does not match configured sink/
  );
  assert.equal(config.store.stats().health.queued, 0);
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
  message.outboxFailureClass = "infrastructure";
  message.outboxError = "connection timed out";
  await harness.input(settle, message);

  const control = config.store.getSinkControl("postgres");
  assert.equal(settle.sent[0][0].outboxSettlement.state, "pending");
  assert.equal(control.consecutiveFailures, 1);
  assert.equal(control.pausedUntil > Date.now(), true);
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
  assert.equal(enqueue.sent[0].outboxEnqueue.inserted, 1);
  await harness.input(claim);
  assert.equal(claim.sent.length, 1);
  assert.deepEqual(claim.sent[0].payload, [{ value: 1 }, { value: 2 }]);

  await harness.close(claim);
  await harness.close(config);
});
