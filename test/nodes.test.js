"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const test = require("node:test");
const registerNodes = require("../nodes/outbox");

function createHarness() {
  const types = new Map();
  const instances = new Map();
  let idCounter = 0;
  const adminRoutes = { get: new Map(), post: new Map() };

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
    auth: {
      needsPermission() {
        return (request, response, next) => next();
      },
    },
    httpAdmin: {
      get(path, ...handlers) {
        adminRoutes.get.set(path, handlers);
      },
      post(path, ...handlers) {
        adminRoutes.post.set(path, handlers);
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

  return { instantiate, input, close, types, adminRoutes };
}

test("registers and exercises config, enqueue, claim, and settle nodes", async () => {
  const harness = createHarness();
  assert.deepEqual(
    [...harness.types.keys()].sort(),
    [
      "durable-outbox-config",
      "outbox-claim",
      "outbox-enqueue",
      "outbox-settle",
    ]
  );
  assert.equal(harness.adminRoutes.get.has("/durable-outbox/:id/status"), true);
  assert.equal(
    harness.adminRoutes.post.has(
      "/durable-outbox/:id/dead-letters/requeue"
    ),
    true
  );

  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    id: "enqueue",
    outbox: "outbox",
    jobProperty: "jobs",
  });
  const claim = harness.instantiate("outbox-claim", {
    id: "claim",
    outbox: "outbox",
    sink: "postgres",
    intervalMs: 0,
    leaseMs: 60_000,
    maxInFlight: 1,
  });
  const settle = harness.instantiate("outbox-settle", {
    id: "settle",
    outbox: "outbox",
    outcome: "success",
  });

  await harness.input(enqueue, {
    jobs: {
      sink: "postgres",
      dedupeKey: "node-test",
      payload: { deviceId: "sensor-1", value: 42 },
    },
  });
  assert.equal(enqueue.sent[0].outboxEnqueue.inserted, 1);

  await harness.input(claim);
  const delivery = claim.sent[0];
  assert.deepEqual(delivery.payload, { deviceId: "sensor-1", value: 42 });
  assert.equal(delivery.outbox.attempts, 1);

  await harness.input(settle, delivery);
  assert.equal(settle.sent[0][0].outboxSettlement.state, "delivered");
  assert.equal(settle.sent[0][1], null);

  await harness.close(claim);
  await harness.close(config);
});

test("settle sends non-retryable failures to its dead-letter output", async () => {
  const harness = createHarness();
  const config = harness.instantiate("durable-outbox-config", {
    id: "outbox",
    filename: ":memory:",
  });
  const enqueue = harness.instantiate("outbox-enqueue", {
    outbox: "outbox",
    jobProperty: "outboxJobs",
  });
  const claim = harness.instantiate("outbox-claim", {
    outbox: "outbox",
    sink: "fieldkit",
    intervalMs: 0,
  });
  const settle = harness.instantiate("outbox-settle", {
    outbox: "outbox",
    outcome: "dead",
  });

  await harness.input(enqueue, {
    outboxJobs: { sink: "fieldkit", payload: { invalid: true } },
  });
  await harness.input(claim);
  const message = claim.sent[0];
  message.outboxError = "HTTP 400";
  message.statusCode = 400;
  await harness.input(settle, message);

  assert.equal(settle.sent[0][0], null);
  assert.equal(settle.sent[0][1].outboxSettlement.state, "dead");
  assert.equal(config.store.listDeadLetters().length, 1);

  await harness.close(claim);
  await harness.close(config);
});
