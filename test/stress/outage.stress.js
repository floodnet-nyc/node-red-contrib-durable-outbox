"use strict";

const assert = require("node:assert/strict");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { monitorEventLoopDelay, performance } = require("node:perf_hooks");
const { setTimeout: delay } = require("node:timers/promises");
const test = require("node:test");
const { createHarness } = require("../helpers/node-red-harness");
const { SimulatedSink } = require("../helpers/simulated-sink");

const JOBS = positiveInteger(process.env.STRESS_JOBS, 5_000);
const BATCH_SIZE = positiveInteger(process.env.STRESS_BATCH_SIZE, 25);
const MAX_IN_FLIGHT = positiveInteger(process.env.STRESS_MAX_IN_FLIGHT, 100);
const MAX_SUITE_MS = positiveInteger(process.env.STRESS_MAX_SUITE_MS, 120_000);
const MAX_EVENT_LOOP_P99_MS = positiveNumber(
  process.env.STRESS_MAX_EVENT_LOOP_P99_MS,
  250
);

function positiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function queuedCount(store) {
  return store.countQueued("simulated");
}

async function settleMessages(harness, settle, simulator, messages) {
  const processed = await Promise.all(
    messages.map((message) => simulator.process(message))
  );
  for (const message of processed) {
    await harness.input(settle, message);
    await new Promise((resolve) => setImmediate(resolve));
  }
}

async function claimAvailable(harness, claim) {
  const messages = [];
  for (;;) {
    const before = claim.sent.length;
    await harness.input(claim);
    if (claim.sent.length === before) break;
    messages.push(...claim.sent.splice(before));
  }
  return messages;
}

test(
  "simulated outage builds a durable backlog and recovery drains every job",
  { timeout: MAX_SUITE_MS },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "outbox-outage-stress-"));
    const harness = createHarness();
    const histogram = monitorEventLoopDelay({ resolution: 10 });
    histogram.enable();
    await delay(20);

    const config = harness.instantiate("durable-outbox-config", {
      id: "outbox",
      filename: join(directory, "outbox.sqlite"),
      maxQueuedJobs: JOBS + 1_000,
      maxJobMb: 1,
      maxDatabaseMb: 1_024,
    });
    harness.instantiate("outbox-sink-config", {
      id: "simulated-sink",
      outbox: "outbox",
      sinkKey: "simulated",
      leaseMs: 5_000,
      maxInFlight: MAX_IN_FLIGHT,
      batchSize: BATCH_SIZE,
      maxAttempts: 3,
      retryUntilExpired: true,
      maxAgeMs: 3_600_000,
      baseDelayMs: 1,
      maxDelayMs: 10,
      circuitBreakerEnabled: true,
      circuitBreakerThreshold: 2,
      circuitBreakerCooldownMs: 60_000,
    });
    const enqueue = harness.instantiate("outbox-enqueue", {
      sink: "simulated-sink",
    });
    const claim = harness.instantiate("outbox-claim", {
      sink: "simulated-sink",
      outputMode: "batch",
    });
    const settle = harness.instantiate("outbox-settle", {
      sink: "simulated-sink",
      outcome: "success",
    });
    const control = harness.instantiate("outbox-control", {
      sink: "simulated-sink",
      action: "status",
    });

    t.after(async () => {
      histogram.disable();
      await harness.close(enqueue);
      await harness.close(claim);
      await harness.close(config);
      rmSync(directory, { recursive: true, force: true });
    });

    const startedAt = performance.now();
    const enqueueStartedAt = performance.now();
    for (let index = 0; index < JOBS; index += 1) {
      await harness.input(enqueue, {
        payload: { sequence: index, bytes: "x".repeat(128) },
        outbox: { dedupeKey: `stress-${index}` },
      });
      if (index % 100 === 99) {
        await new Promise((resolve) => setImmediate(resolve));
      }
    }
    const enqueueMs = performance.now() - enqueueStartedAt;
    assert.equal(queuedCount(config.store), JOBS);

    const simulator = new SimulatedSink({
      mode: "outage",
      latencyMs: 2,
      jitterMs: 2,
      random: () => 0.5,
    });
    const outageClaims = await claimAvailable(harness, claim);
    assert.equal(
      outageClaims.reduce(
        (total, message) => total + message.outbox.batch.length,
        0
      ),
      Math.min(JOBS, MAX_IN_FLIGHT)
    );
    await settleMessages(harness, settle, simulator, outageClaims);

    const openCircuit = config.store.getSinkControl("simulated");
    assert.equal(openCircuit.pausedUntil != null, true);
    assert.equal(queuedCount(config.store), JOBS);
    assert.equal(config.store.listDeadLetters({ limit: 1 }).length, 0);

    const sentBeforeOpenPoll = claim.sent.length;
    await harness.input(claim);
    assert.equal(claim.sent.length, sentBeforeOpenPoll);

    simulator.setMode("healthy");
    await harness.input(control, { outbox: { action: "resume" } });

    const drainStartedAt = performance.now();
    while (queuedCount(config.store) > 0) {
      const messages = await claimAvailable(harness, claim);
      if (!messages.length) {
        await delay(2);
        continue;
      }
      await settleMessages(harness, settle, simulator, messages);
    }
    const drainMs = performance.now() - drainStartedAt;
    const totalMs = performance.now() - startedAt;
    await delay(20);

    const stats = config.store.stats();
    const delivered = stats.jobs.find(
      (row) => row.sink === "simulated" && row.state === "delivered"
    );
    const p99EventLoopMs = histogram.percentile(99) / 1e6;
    const metrics = {
      jobs: JOBS,
      batchSize: BATCH_SIZE,
      maxInFlight: MAX_IN_FLIGHT,
      enqueueMs: Math.round(enqueueMs),
      enqueueJobsPerSecond: Math.round((JOBS * 1_000) / enqueueMs),
      drainMs: Math.round(drainMs),
      drainJobsPerSecond: Math.round((JOBS * 1_000) / drainMs),
      totalMs: Math.round(totalMs),
      eventLoopP99Ms: Number(p99EventLoopMs.toFixed(2)),
      simulatorMaxConcurrency: simulator.maxActive,
      simulatedFailedBatches: simulator.failedAttempts,
    };
    process.stdout.write(`STRESS_METRICS ${JSON.stringify(metrics)}\n`);

    assert.equal(stats.health.queued, 0);
    assert.equal(stats.deadLetters, 0);
    assert.equal(delivered?.count, JOBS);
    assert.equal(simulator.successfulJobIds.size, JOBS);
    assert.equal(
      [...simulator.successfulJobDeliveries.values()].every(
        (deliveries) => deliveries === 1
      ),
      true
    );
    assert.ok(totalMs <= MAX_SUITE_MS, `stress run took ${totalMs}ms`);
    assert.ok(
      p99EventLoopMs <= MAX_EVENT_LOOP_P99_MS,
      `event-loop p99 ${p99EventLoopMs}ms exceeded ${MAX_EVENT_LOOP_P99_MS}ms`
    );
  }
);

test(
  "a slow simulated delivery is fenced after its lease is reclaimed",
  { timeout: 30_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "outbox-stale-stress-"));
    const harness = createHarness();
    const config = harness.instantiate("durable-outbox-config", {
      id: "outbox",
      filename: join(directory, "outbox.sqlite"),
    });
    harness.instantiate("outbox-sink-config", {
      id: "simulated-sink",
      outbox: "outbox",
      sinkKey: "simulated",
      leaseMs: 20,
      maxInFlight: BATCH_SIZE,
      batchSize: BATCH_SIZE,
      retryUntilExpired: true,
    });
    const enqueue = harness.instantiate("outbox-enqueue", {
      sink: "simulated-sink",
    });
    const claim = harness.instantiate("outbox-claim", {
      sink: "simulated-sink",
      outputMode: "batch",
    });
    const settle = harness.instantiate("outbox-settle", {
      sink: "simulated-sink",
      outcome: "success",
    });

    t.after(async () => {
      await harness.close(enqueue);
      await harness.close(claim);
      await harness.close(config);
      rmSync(directory, { recursive: true, force: true });
    });

    for (let index = 0; index < BATCH_SIZE; index += 1) {
      await harness.input(enqueue, {
        payload: { index },
        outbox: { dedupeKey: `stale-${index}` },
      });
    }
    await harness.input(claim);
    const staleMessage = claim.sent.shift();
    const slow = new SimulatedSink({ latencyMs: 50 });
    const slowDelivery = slow.process(staleMessage);

    await delay(30);
    await harness.input(claim);
    const currentMessage = claim.sent.shift();
    assert.ok(currentMessage, "expired leases should be reclaimed");
    const fast = new SimulatedSink();
    await harness.input(settle, await fast.process(currentMessage));

    await harness.input(settle, await slowDelivery);
    const staleResult = settle.sent.at(-1)[0].outbox.result;
    assert.equal(staleResult.stale.length, BATCH_SIZE);
    assert.equal(staleResult.delivered.length, 0);
    assert.equal(queuedCount(config.store), 0);
    assert.equal(fast.successfulJobIds.size, BATCH_SIZE);
  }
);
