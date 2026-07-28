"use strict";

const assert = require("node:assert/strict");
const { fork } = require("node:child_process");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { performance } = require("node:perf_hooks");
const { setTimeout: delay } = require("node:timers/promises");
const test = require("node:test");
const { OutboxStore } = require("../../lib/outbox-store");

const JOBS = positiveInteger(process.env.STRESS_JOBS, 5_000);
const EXPIRED_JOBS = positiveInteger(
  process.env.STRESS_EXPIRED_JOBS,
  Math.max(5_000, JOBS)
);
const MIN_JOBS_PER_SECOND = positiveNumber(
  process.env.STRESS_MIN_JOBS_PER_SECOND,
  100
);
const MAX_TEST_MS = positiveInteger(process.env.STRESS_MAX_STORE_MS, 120_000);

function positiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function percentile(values, fraction) {
  if (!values.length) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))];
}

function temporaryStore(t, prefix, options = {}) {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  const store = new OutboxStore(join(directory, "outbox.sqlite"), options);
  t.after(() => {
    store.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return store;
}

test(
  "store sustains a growing backlog and drains it without loss",
  { timeout: MAX_TEST_MS },
  (t) => {
    const store = temporaryStore(t, "outbox-store-stress-", {
      maxQueuedJobs: JOBS + 1_000,
      maxDatabaseBytes: 2 * 1_024 * 1_024 * 1_024,
    });
    const enqueueLatencies = [];
    const windows = [];
    const windowSize = Math.max(100, Math.floor(JOBS / 5));
    let windowStartedAt = performance.now();
    const enqueueStartedAt = windowStartedAt;

    for (let index = 0; index < JOBS; index += 1) {
      const operationStartedAt = performance.now();
      const [result] = store.enqueue({
        sink: "stress",
        dedupeKey: `store-stress-${index}`,
        payload: { sequence: index, bytes: "x".repeat(128) },
      });
      enqueueLatencies.push(performance.now() - operationStartedAt);
      assert.equal(result.inserted, true);

      if ((index + 1) % windowSize === 0 || index + 1 === JOBS) {
        const now = performance.now();
        const jobsInWindow =
          (index + 1) % windowSize || Math.min(windowSize, index + 1);
        windows.push({
          backlog: index + 1,
          jobsPerSecond: (jobsInWindow * 1_000) / (now - windowStartedAt),
        });
        windowStartedAt = now;
      }
    }
    const enqueueMs = performance.now() - enqueueStartedAt;
    assert.equal(store.stats().health.queued, JOBS);

    const claimedIds = new Set();
    const drainStartedAt = performance.now();
    for (;;) {
      const batch = store.claimBatch({
        sink: "stress",
        batchSize: 100,
        maxInFlight: 100,
        leaseMs: 60_000,
      });
      if (!batch.length) break;
      for (const job of batch) {
        assert.equal(claimedIds.has(job.id), false);
        claimedIds.add(job.id);
        assert.equal(
          store.settle(job.id, {
            leaseToken: job.leaseToken,
            success: true,
          }).state,
          "delivered"
        );
      }
    }
    const drainMs = performance.now() - drainStartedAt;
    const stats = store.stats();
    const delivered = stats.jobs.find(
      (row) => row.sink === "stress" && row.state === "delivered"
    );
    const metrics = {
      jobs: JOBS,
      enqueueMs: Math.round(enqueueMs),
      enqueueJobsPerSecond: Math.round((JOBS * 1_000) / enqueueMs),
      enqueueP95Ms: Number(percentile(enqueueLatencies, 0.95).toFixed(3)),
      enqueueP99Ms: Number(percentile(enqueueLatencies, 0.99).toFixed(3)),
      enqueueWindows: windows.map((window) => ({
        backlog: window.backlog,
        jobsPerSecond: Math.round(window.jobsPerSecond),
      })),
      drainMs: Math.round(drainMs),
      drainJobsPerSecond: Math.round((JOBS * 1_000) / drainMs),
      databaseBytes: stats.health.databaseBytes,
      walBytes: stats.health.walBytes,
    };
    process.stdout.write(`STRESS_METRICS ${JSON.stringify(metrics)}\n`);

    assert.equal(claimedIds.size, JOBS);
    assert.equal(delivered?.count, JOBS);
    assert.equal(stats.health.queued, 0);
    assert.equal(stats.deadLetters, 0);
    assert.ok(
      metrics.enqueueJobsPerSecond >= MIN_JOBS_PER_SECOND,
      `enqueue rate ${metrics.enqueueJobsPerSecond}/s was below ${MIN_JOBS_PER_SECOND}/s`
    );
  }
);

test(
  "an expired-lease storm completes without losing jobs",
  { timeout: MAX_TEST_MS },
  (t) => {
    let now = 1_000_000;
    const store = temporaryStore(t, "outbox-expiry-stress-", {
      now: () => now,
      maxQueuedJobs: EXPIRED_JOBS + 1_000,
    });
    const batchSize = 1_000;
    for (let offset = 0; offset < EXPIRED_JOBS; offset += batchSize) {
      const count = Math.min(batchSize, EXPIRED_JOBS - offset);
      store.enqueue(
        Array.from({ length: count }, (_, batchIndex) => {
          const index = offset + batchIndex;
          return {
            sink: "stress",
            dedupeKey: `expired-${index}`,
            payload: { index },
            maxAttempts: 1,
          };
        })
      );
    }

    let leased = 0;
    while (leased < EXPIRED_JOBS) {
      const batch = store.claimBatch({
        sink: "stress",
        batchSize,
        maxInFlight: EXPIRED_JOBS,
        leaseMs: 100,
      });
      assert.ok(batch.length, "all queued jobs should be leased");
      leased += batch.length;
    }

    now += 101;
    const cleanupStartedAt = performance.now();
    let cleanupSweeps = 0;
    let previousDeadLetters = 0;
    while (previousDeadLetters < EXPIRED_JOBS) {
      assert.deepEqual(store.claimBatch({
        sink: "stress",
        batchSize,
        maxInFlight: EXPIRED_JOBS,
        leaseMs: 100,
      }), []);
      cleanupSweeps += 1;
      const currentDeadLetters = store.stats().deadLetters;
      assert.ok(
        currentDeadLetters - previousDeadLetters <= 1_000,
        "an exhaustion sweep must remain bounded"
      );
      assert.ok(
        currentDeadLetters > previousDeadLetters,
        "each sweep should make cleanup progress"
      );
      previousDeadLetters = currentDeadLetters;
    }
    const cleanupMs = performance.now() - cleanupStartedAt;
    const stats = store.stats();
    process.stdout.write(
      `STRESS_METRICS ${JSON.stringify({
        scenario: "expired-lease-storm",
        jobs: EXPIRED_JOBS,
        cleanupSweeps,
        cleanupMs: Math.round(cleanupMs),
        deadLetters: stats.deadLetters,
      })}\n`
    );

    assert.equal(stats.health.queued, 0);
    assert.equal(stats.deadLetters, EXPIRED_JOBS);
  }
);

test(
  "a killed worker leaves a recoverable WAL database and fenced leases",
  { timeout: 30_000 },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "outbox-crash-stress-"));
    const filename = join(directory, "outbox.sqlite");
    const jobCount = Math.min(Math.max(1_000, Math.floor(JOBS / 5)), 10_000);
    let store = new OutboxStore(filename, {
      maxQueuedJobs: jobCount + 1_000,
    });
    try {
      for (let offset = 0; offset < jobCount; offset += 1_000) {
        store.enqueue(
          Array.from(
            { length: Math.min(1_000, jobCount - offset) },
            (_, batchIndex) => {
              const index = offset + batchIndex;
              return {
                sink: "stress",
                dedupeKey: `crash-${index}`,
                payload: { index },
              };
            }
          )
        );
      }
      store.close();
      store = null;

      const leaseMs = 100;
      const worker = fork(
        join(__dirname, "../helpers/crash-worker.js"),
        [filename, "stress", "100", String(leaseMs)],
        { stdio: ["ignore", "ignore", "ignore", "ipc"] }
      );
      const leased = await new Promise((resolve, reject) => {
        worker.once("message", (message) => resolve(message.leased));
        worker.once("error", reject);
        worker.once("exit", (code, signal) => {
          if (code !== null && code !== 0) {
            reject(new Error(`crash worker exited early with ${code}/${signal}`));
          }
        });
      });
      assert.equal(leased, 100);
      const workerExited = new Promise((resolve) =>
        worker.once("exit", resolve)
      );
      worker.kill("SIGKILL");
      await workerExited;
      await delay(leaseMs + 25);

      store = new OutboxStore(filename, {
        maxQueuedJobs: jobCount + 1_000,
      });
      assert.equal(
        store.db.prepare("PRAGMA integrity_check").get().integrity_check,
        "ok"
      );
      let delivered = 0;
      for (;;) {
        const jobs = store.claimBatch({
          sink: "stress",
          batchSize: 100,
          maxInFlight: 100,
          leaseMs: 10_000,
        });
        if (!jobs.length) break;
        const results = store.settleBatch(
          jobs.map((job) => ({
            id: job.id,
            outcome: { leaseToken: job.leaseToken, success: true },
          }))
        );
        delivered += results.filter(
          (result) => result.state === "delivered"
        ).length;
      }
      assert.equal(delivered, jobCount);
      assert.equal(store.stats().health.queued, 0);
      process.stdout.write(
        `STRESS_METRICS ${JSON.stringify({
          scenario: "killed-worker-recovery",
          jobs: jobCount,
          abandonedLeases: leased,
          delivered,
        })}\n`
      );
    } finally {
      if (store) store.close();
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
