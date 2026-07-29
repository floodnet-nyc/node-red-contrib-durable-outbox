"use strict";

const { OutboxStore } = require("../lib/outbox-store");
const D = require("../lib/defaults");

module.exports = function registerOutboxNodes(RED) {
  function megabytesToBytes(value, fallbackMb) {
    const megabytes = Number(value);
    const normalized =
      Number.isFinite(megabytes) && megabytes > 0 ? megabytes : fallbackMb;
    return Math.max(1, Math.round(normalized * 1_048_576));
  }

  function nonnegativeMegabytesToBytes(value, fallbackMb) {
    const megabytes = Number(value);
    const normalized =
      Number.isFinite(megabytes) && megabytes >= 0 ? megabytes : fallbackMb;
    return Math.max(0, Math.round(normalized * 1_048_576));
  }

  function nonnegativeNumber(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : fallback;
  }

  function booleanValue(value, fallback) {
    if (value == null || value === "") return fallback;
    return value !== false && value !== "false";
  }

  function compactDuration(value, { roundUp = false } = {}) {
    const milliseconds = Math.max(0, Number(value) || 0);
    const select = roundUp ? Math.ceil : Math.floor;
    if (milliseconds < 1_000) return "<1s";
    if (milliseconds < 60_000) {
      return `${Math.max(1, select(milliseconds / 1_000))}s`;
    }
    if (milliseconds < 3_600_000) {
      return `${Math.max(1, select(milliseconds / 60_000))}m`;
    }
    if (milliseconds < 86_400_000) {
      return `${Math.max(1, select(milliseconds / 3_600_000))}h`;
    }
    return `${Math.max(1, select(milliseconds / 86_400_000))}d`;
  }

  function compactSinkDisplay(status) {
    const tokens = [`q${status.queued}`];
    const appearances = {
      ready: { fill: "green", shape: "dot" },
      run: { fill: "blue", shape: "dot" },
      wait: { fill: "yellow", shape: "ring" },
      paused: { fill: "blue", shape: "ring" },
      open: { fill: "red", shape: "ring" },
      probe: { fill: "yellow", shape: "dot" },
      stuck: { fill: "red", shape: "ring" },
      dead: { fill: "red", shape: "dot" },
      pressure: { fill: "yellow", shape: "ring" },
      capacity: { fill: "red", shape: "ring" },
    };
    const appearance = appearances[status.state] || {
      fill: "grey",
      shape: "ring",
    };

    if (status.state === "ready") {
      tokens.push("ready");
    } else if (status.state === "wait") {
      tokens.push(
        status.nextAvailableInMs == null
          ? "wait"
          : `wait ${compactDuration(status.nextAvailableInMs, {
              roundUp: true,
            })}`
      );
    } else if (status.state === "paused") {
      tokens.push("paused ⏸");
    } else if (status.state === "open") {
      tokens.push(
        `open ${compactDuration(status.retryInMs, { roundUp: true })} ⚡`
      );
    } else if (status.state === "probe") {
      tokens.push("probe");
    } else if (status.state === "stuck") {
      tokens.push("stuck");
    } else if (status.state === "pressure") {
      tokens.push("pressure 💾");
    } else if (status.state === "capacity") {
      tokens.push("capacity 💾");
    }

    if (status.oldestQueuedAgeMs != null) {
      tokens.push(`age ${compactDuration(status.oldestQueuedAgeMs)}`);
    }
    if (status.leased > 0) tokens.push(`${status.leased} 🪽`);
    if (status.retrying > 0) tokens.push(`${status.retrying} 🥀`);
    if (status.expiredLeases > 0) {
      tokens.push(`${status.expiredLeases} expired`);
    }
    if (status.deadLetters > 0) tokens.push(`${status.deadLetters} ☠️`);
    if (status.state === "pressure" || status.state === "capacity") {
      tokens.push(`disk ${Math.round(status.health.databaseUsedPercent)}%`);
    }

    return {
      ...appearance,
      text: tokens.join(" · "),
    };
  }

  function DurableOutboxConfigNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    this.filename = config.filename;
    this.cleanupIntervalMs = Math.max(
      1_000,
      nonnegativeNumber(config.cleanupIntervalMs, D.CLEANUP_INTERVAL_MS)
    );
    const cleanupHighWatermarkPercent = nonnegativeNumber(
      config.cleanupHighWatermarkPercent,
      D.CLEANUP_HIGH_WATERMARK * 100
    );
    const cleanupLowWatermarkPercent = nonnegativeNumber(
      config.cleanupLowWatermarkPercent,
      D.CLEANUP_LOW_WATERMARK * 100
    );
    if (
      cleanupHighWatermarkPercent <= 0 ||
      cleanupHighWatermarkPercent >= 100 ||
      cleanupLowWatermarkPercent >= cleanupHighWatermarkPercent
    ) {
      throw new Error(
        "Cleanup percentages must satisfy 0 <= target < start < 100"
      );
    }
    const storeOptions = {
      maxQueuedJobs: config.maxQueuedJobs,
      maxJobBytes: megabytesToBytes(config.maxJobMb, 1),
      maxEnqueueBatch: config.maxEnqueueBatch,
      maxDatabaseBytes: megabytesToBytes(config.maxDatabaseMb, 1_024),
      minFreeDiskBytes: nonnegativeMegabytesToBytes(
        config.minFreeDiskMb,
        256
      ),
      deliveredRetentionMs: nonnegativeNumber(
        config.deliveredRetentionMs,
        D.DELIVERED_RETENTION_MS
      ),
      cleanupBatchSize: config.cleanupBatchSize,
      cleanupHighWatermark: cleanupHighWatermarkPercent / 100,
      cleanupLowWatermark: cleanupLowWatermarkPercent / 100,
      protectIngestion: booleanValue(config.protectIngestion, true),
    };
    try {
      this.store = new OutboxStore(this.filename, storeOptions);
    } catch (error) {
      this.error(`Unable to open durable outbox: ${error.message}`);
      throw error;
    }

    let closed = false;
    let cleanupTimer = null;
    const scheduleCleanup = (delay) => {
      cleanupTimer = setTimeout(runCleanup, delay);
      cleanupTimer.unref?.();
    };
    const runCleanup = () => {
      if (closed) return;
      try {
        const result = node.store.cleanupDelivered();
        scheduleCleanup(result.needsMore ? 10 : node.cleanupIntervalMs);
      } catch (error) {
        node.error(`Automatic outbox cleanup failed: ${error.message}`);
        scheduleCleanup(node.cleanupIntervalMs);
      }
    };
    scheduleCleanup(this.cleanupIntervalMs);

    this.on("close", (removed, done) => {
      try {
        closed = true;
        if (cleanupTimer) clearTimeout(cleanupTimer);
        this.store.close();
        done();
      } catch (error) {
        done(error);
      }
    });
  }

  function getOutboxConfigNode(runtimeNode, id) {
    const configNode = RED.nodes.getNode(id);
    if (!configNode?.store) {
      runtimeNode.status({ fill: "red", shape: "ring", text: "not configured" });
      throw new Error("A valid durable outbox configuration is required");
    }
    return configNode;
  }

  function positiveNumber(value, fallback) {
    return Math.max(1, Number(value) || fallback);
  }

  function OutboxSinkConfigNode(config) {
    RED.nodes.createNode(this, config);
    const outboxConfig = getOutboxConfigNode(this, config.outbox);
    const sinkKey = String(config.sinkKey || "").trim();
    if (!sinkKey) throw new Error("A stable sink key is required");

    this.outbox = config.outbox;
    this.outboxConfig = outboxConfig;
    this.store = outboxConfig.store;
    this.sinkKey = sinkKey;
    this.leaseMs = positiveNumber(config.leaseMs, D.LEASE_MS);
    this.maxInFlight = positiveNumber(config.maxInFlight, D.MAX_IN_FLIGHT);
    this.batchSize = positiveNumber(config.batchSize, D.BATCH_SIZE);
    this.maxAttempts = positiveNumber(config.maxAttempts, D.MAX_ATTEMPTS);
    this.retryUntilExpired = booleanValue(
      config.retryUntilExpired,
      D.RETRY_UNTIL_EXPIRED
    );
    this.maxAgeMs = positiveNumber(config.maxAgeMs, D.MAX_AGE_MS);
    this.baseDelayMs = positiveNumber(config.baseDelayMs, D.BASE_DELAY_MS);
    this.maxDelayMs = positiveNumber(config.maxDelayMs, D.MAX_DELAY_MS);
    this.circuitBreakerEnabled =
      config.circuitBreakerEnabled !== false &&
      config.circuitBreakerEnabled !== "false";
    this.circuitBreakerThreshold = positiveNumber(
      config.circuitBreakerThreshold,
      D.CIRCUIT_THRESHOLD
    );
    this.circuitBreakerCooldownMs = positiveNumber(
      config.circuitBreakerCooldownMs,
      D.CIRCUIT_COOLDOWN_MS
    );
    const queueDepthListeners = new Set();
    this.subscribeQueueDepth = (listener) => {
      queueDepthListeners.add(listener);
      listener(this.store.countQueued(this.sinkKey));
      return () => queueDepthListeners.delete(listener);
    };
    this.notifyQueueDepth = () => {
      const count = this.store.countQueued(this.sinkKey);
      for (const listener of queueDepthListeners) listener(count);
      return count;
    };
  }

  function getSinkConfigNode(runtimeNode, id, expectedOutbox) {
    const sinkConfig = RED.nodes.getNode(id);
    if (!sinkConfig?.store || !sinkConfig?.sinkKey) {
      runtimeNode.status({ fill: "red", shape: "ring", text: "no sink" });
      throw new Error("A valid outbox sink configuration is required");
    }
    if (
      expectedOutbox &&
      sinkConfig.outboxConfig !== expectedOutbox
    ) {
      throw new Error("The sink and node must use the same durable outbox");
    }
    return sinkConfig;
  }

  function OutboxEnqueueNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const sinkConfig = getSinkConfigNode(node, config.sink);
    const payloadProperty = config.payloadProperty || "payload";
    const STATUS_THROTTLE_MS = 500;
    const CAPACITY_CODES = new Set([
      "OUTBOX_DATABASE_CAPACITY",
      "OUTBOX_DISK_HEADROOM",
      "OUTBOX_QUEUE_CAPACITY",
    ]);
    let lastDepthStatus = -STATUS_THROTTLE_MS;
    let capacityEpisode = null;
    const unsubscribeQueueDepth = sinkConfig.subscribeQueueDepth((count) => {
      const now = Date.now();
      if (count !== 0 && now - lastDepthStatus < STATUS_THROTTLE_MS) return;
      if (count !== 0) lastDepthStatus = now;
      node.status({
        fill: "green",
        shape: "dot",
        text: `${count} queued`,
      });
    });

    node.on("input", (msg, send, done) => {
      send = send || node.send.bind(node);
      try {
        const payload = RED.util.getMessageProperty(msg, payloadProperty);
        if (payload === undefined) {
          throw new Error(
            `msg.${payloadProperty} does not contain an outbox payload`
          );
        }
        const options = msg.outbox;
        if (
          options != null &&
          (typeof options !== "object" || Array.isArray(options))
        ) {
          throw new TypeError("msg.outbox must be an object when provided");
        }
        if (options?.sink != null && options.sink !== sinkConfig.sinkKey) {
          throw new Error(
            `Job sink ${options.sink} does not match configured sink ${sinkConfig.sinkKey}`
          );
        }
        const job = {
          maxAttempts: sinkConfig.maxAttempts,
          retryUntilExpired: sinkConfig.retryUntilExpired,
          maxAgeMs: sinkConfig.maxAgeMs,
          baseDelayMs: sinkConfig.baseDelayMs,
          maxDelayMs: sinkConfig.maxDelayMs,
          ...(options || {}),
          sink: sinkConfig.sinkKey,
          payload,
        };
        const results = sinkConfig.store.enqueue(job);
        if (!msg.outbox) msg.outbox = {};
        msg.outbox.result = {
          sink: sinkConfig.sinkKey,
          jobs: results,
          inserted: results.filter((job) => job.inserted).length,
          duplicates: results.filter((job) => !job.inserted).length,
          queueDepth: sinkConfig.notifyQueueDepth(),
          cleanup: results.cleanup,
        };
        capacityEpisode = null;
        send(msg);
        done();
      } catch (error) {
        if (CAPACITY_CODES.has(error.code)) {
          const now = Date.now();
          if (
            !capacityEpisode ||
            now - capacityEpisode.lastReportedAt >=
              D.CAPACITY_LOG_INTERVAL_MS
          ) {
            const suppressed = capacityEpisode?.suppressed || 0;
            node.error(
              suppressed
                ? `${error.message} (${suppressed} similar rejections suppressed)`
                : error
            );
            capacityEpisode = {
              code: error.code,
              startedAt: capacityEpisode?.startedAt || now,
              lastReportedAt: now,
              rejected: (capacityEpisode?.rejected || 0) + 1,
              suppressed: 0,
            };
          } else {
            capacityEpisode.rejected += 1;
            capacityEpisode.suppressed += 1;
          }
          const storage =
            error.health || sinkConfig.store.storageHealth({
              refreshDisk: true,
            });
          if (!msg.outbox || typeof msg.outbox !== "object") {
            msg.outbox = {};
          }
          msg.outbox.result = {
            sink: sinkConfig.sinkKey,
            inserted: false,
            error: {
              code: error.code,
              message: error.message,
            },
            queueDepth: sinkConfig.store.countQueued(sinkConfig.sinkKey),
            health: {
              ...storage,
              databaseUsedPercent:
                sinkConfig.store.databaseUsedRatio(storage) * 100,
            },
            cleanup: {
              attempted: Boolean(error.cleanupAttempted),
              batches: Number(error.cleanupBatches) || 0,
              purged: Number(error.deliveredPurged) || 0,
            },
          };
          node.status({
            fill: "red",
            shape: "ring",
            text: `capacity blocked (${capacityEpisode.rejected})`,
          });
          send([null, msg]);
          done();
          return;
        }
        node.status({ fill: "red", shape: "ring", text: "enqueue failed" });
        done(error);
      }
    });

    node.on("close", (removed, done) => {
      unsubscribeQueueDepth();
      done();
    });
  }

  function OutboxClaimNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const sinkConfig = getSinkConfigNode(node, config.sink);
    const { store, sinkKey: sink, leaseMs, maxInFlight, batchSize } =
      sinkConfig;
    const outputMode = config.outputMode || "individual";

    function claim(send, done) {
      try {
        const jobs = store.claimBatch({
          sink,
          leaseMs,
          maxInFlight,
          batchSize,
          circuitBreakerEnabled: sinkConfig.circuitBreakerEnabled,
        });
        sinkConfig.notifyQueueDepth();
        const quarantined = Number(jobs.quarantined) || 0;
        if (jobs.length) {
          node.status({
            fill: "blue",
            shape: "dot",
            text: quarantined
              ? `${jobs.length} leased, ${quarantined} quarantined`
              : `${jobs.length} leased`,
          });
          if (outputMode === "batch") {
            send({
              _msgid: RED.util.generateId(),
              payload: jobs.map((job) => job.payload),
              outbox: {
                batch: jobs.map((job) => {
                  const { payload, ...metadata } = job;
                  return metadata;
                }),
              },
            });
          } else {
            for (const job of jobs) {
              send({
                _msgid: RED.util.generateId(),
                payload: job.payload,
                outbox: job,
              });
            }
          }
        } else if (quarantined) {
          node.status({
            fill: "red",
            shape: "dot",
            text: `${quarantined} quarantined`,
          });
        } else {
          const control = store.getSinkControl(sink);
          if (control.manuallyPaused) {
            node.status({ fill: "yellow", shape: "dot", text: "paused" });
          } else if (
            sinkConfig.circuitBreakerEnabled &&
            control.pausedUntil != null &&
            control.pausedUntil > sinkConfig.store.now()
          ) {
            node.status({
              fill: "yellow",
              shape: "ring",
              text: "circuit open",
            });
          } else {
            node.status({ fill: "green", shape: "dot", text: "" });
          }
        }
        if (done) done();
      } catch (error) {
        node.status({ fill: "red", shape: "ring", text: "claim failed" });
        if (done) done(error);
        else node.error(error);
      }
    }

    node.on("input", (msg, send, done) => {
      claim(send || node.send.bind(node), done);
    });

    node.on("close", (removed, done) => {
      done();
    });
  }

  function OutboxSettleNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const sinkConfig = getSinkConfigNode(node, config.sink);
    const configuredOutcome = config.outcome || "success";

    function resolveOutcome(msg) {
      let success;
      let retryable;

      if (msg.outbox?.outcome === "success") {
        success = true;
      } else if (msg.outbox?.retryable != null) {
        success = false;
        retryable = msg.outbox.retryable;
      } else if (configuredOutcome === "success") {
        success = true;
      } else {
        success = false;
        retryable = configuredOutcome === "retry";
      }

      const error =
        msg.outbox?.error ||
        msg.error ||
        (success ? null : msg.payload?.error) ||
        (success ? null : "Delivery failed");
      const status =
        msg.statusCode ?? msg.outbox?.status ?? msg.payload?.statusCode;
      const failureClass =
        msg.outbox?.failureClass ||
        (retryable ? "infrastructure" : "data");

      let circuitFailure = false;
      if (!success) {
        if (
          msg.outbox?.circuitFailure != null &&
          typeof msg.outbox.circuitFailure !== "boolean"
        ) {
          throw new TypeError("msg.outbox.circuitFailure must be a boolean");
        }
        circuitFailure = msg.outbox?.circuitFailure ?? retryable;
      }
      return { success, retryable, error, status, failureClass, circuitFailure };
    }

    function validateOutbox(outbox) {
      if (!outbox?.id) {
        throw new Error("Every settlement requires an outbox job id");
      }
      if (
        !outbox.leaseToken ||
        typeof outbox.leaseToken !== "string"
      ) {
        throw new Error(`Outbox job ${outbox.id} requires a lease token`);
      }
      if (outbox.sink !== sinkConfig.sinkKey) {
        throw new Error(
          `Job sink ${outbox.sink} does not match configured sink ${sinkConfig.sinkKey}`
        );
      }
    }

    function storeOutcome(outbox, outcome, circuitFailure) {
      return {
        leaseToken: outbox.leaseToken,
        success: outcome.success,
        retryable: outcome.retryable,
        error: outcome.error,
        status: outcome.status,
        failureClass: outcome.failureClass,
        circuitFailure,
        circuitBreakerThreshold: sinkConfig.circuitBreakerThreshold,
        circuitBreakerCooldownMs: sinkConfig.circuitBreakerCooldownMs,
      };
    }

    function settleOutbox(outbox, outcome, circuitFailure) {
      return sinkConfig.store.settle(
        outbox.id,
        storeOutcome(outbox, outcome, circuitFailure)
      );
    }

    node.on("input", (msg, send, done) => {
      send = send || node.send.bind(node);
      try {
        const outcome = resolveOutcome(msg);
        const batch = msg.outbox?.batch;
        if (batch) {
          if (!batch.length) {
            throw new Error("msg.outbox.batch must contain at least one lease");
          }
          for (const lease of batch) validateOutbox(lease);

          const circuitFailure =
            !outcome.success &&
            sinkConfig.circuitBreakerEnabled &&
            outcome.circuitFailure;
          const results = sinkConfig.store.settleBatch(
            batch.map((lease) => ({
              id: lease.id,
              outcome: storeOutcome(lease, outcome, circuitFailure),
            })),
            { circuitFailureOnce: circuitFailure }
          );
          const entries = batch.map((lease, index) => ({
            lease,
            result: results[index],
          }));
          sinkConfig.notifyQueueDepth();

          const summary = {
            delivered: entries
              .filter(({ result }) => result.state === "delivered")
              .map(({ result }) => result),
            retrying: entries
              .filter(({ result }) => result.state === "pending")
              .map(({ result }) => result),
            dead: entries
              .filter(({ result }) => result.state === "dead")
              .map(({ result }) => result),
            stale: entries
              .filter(({ result }) => result.state === "stale_lease")
              .map(({ result }) => result),
          };
          const activeEntries = entries.filter(
            ({ result }) => result.state !== "dead"
          );
          const deadEntries = entries.filter(
            ({ result }) => result.state === "dead"
          );
          const batchMessage = (selected) => ({
            ...msg,
            outbox: {
              ...msg.outbox,
              batch: selected.map(({ lease }) => lease),
              result: summary,
            },
          });

          if (summary.dead.length) {
            node.status({
              fill: "red",
              shape: "dot",
              text: `${summary.dead.length} dead`,
            });
          } else if (summary.retrying.length) {
            node.status({
              fill: "yellow",
              shape: "ring",
              text: `${summary.retrying.length} retrying`,
            });
          } else if (summary.delivered.length) {
            node.status({
              fill: "green",
              shape: "dot",
              text: `${summary.delivered.length} delivered`,
            });
          } else {
            node.status({
              fill: "yellow",
              shape: "ring",
              text: `${summary.stale.length} stale`,
            });
          }
          send([
            activeEntries.length ? batchMessage(activeEntries) : null,
            deadEntries.length ? batchMessage(deadEntries) : null,
          ]);
          done();
          return;
        }

        if (!msg.outbox?.id) {
          throw new Error("msg.outbox.id is required to settle a job");
        }
        validateOutbox(msg.outbox);
        const result = settleOutbox(
          msg.outbox,
          outcome,
          sinkConfig.circuitBreakerEnabled && outcome.circuitFailure
        );
        sinkConfig.notifyQueueDepth();
        msg.outbox.result = result;

        if (result.state === "stale_lease") {
          node.status({ fill: "yellow", shape: "ring", text: "stale lease" });
          send([msg, null]);
        } else if (result.state === "dead") {
          node.status({ fill: "red", shape: "dot", text: "dead letter" });
          send([null, msg]);
        } else if (result.state === "delivered") {
          node.status({ fill: "green", shape: "dot" });
          send([msg, null]);
        } else {
          node.status({
            fill: "yellow",
            shape: "ring",
            text: `retry ${result.attempts}`,
          });
          send([msg, null]);
        }
        done();
      } catch (error) {
        node.status({ fill: "red", shape: "ring", text: "settle failed" });
        done(error);
      }
    });
  }

  function OutboxControlNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const sinkConfig = getSinkConfigNode(node, config.sink);
    const store = sinkConfig.store;

    node.on("input", (msg, send, done) => {
      send = send || node.send.bind(node);
      try {
        const shell = msg.outbox || {};
        const action = shell.action || config.action || "status";
        const sink = shell.sink || sinkConfig.sinkKey;
        const failureClass = shell.failureClass || config.failureClass || undefined;
        const limit = Number(shell.limit ?? config.limit) || 100;
        const requestedAge = Number(
          shell.olderThanMs ?? config.olderThanMs ?? D.MAX_AGE_MS
        );
        const olderThanMs = Number.isFinite(requestedAge)
          ? requestedAge
          : D.MAX_AGE_MS;
        const id = shell.deadLetterId;
        let result;

        switch (action) {
          case "status": {
            result = store.sinkStatus(sink);
            result.display = compactSinkDisplay(result);
            break;
          }
          case "pause":
            if (!sink) throw new Error("A sink is required to pause");
            result = store.pauseSink(sink);
            break;
          case "resume":
            if (!sink) throw new Error("A sink is required to resume");
            result = store.resumeSink(sink, { retryNow: true });
            break;
          case "retry-now":
            if (!sink) throw new Error("A sink is required to retry jobs");
            result = store.retryNow(sink);
            break;
          case "list-dead": {
            const rows = store.listDeadLetters({
              sink,
              failureClass,
              limit,
            });
            result = { jobs: rows, count: rows.length };
            break;
          }
          case "requeue-dead":
            result = store.requeueDeadLetters({
              sink,
              failureClass,
              limit,
            });
            break;
          case "requeue-one":
            if (!id) {
              throw new Error("msg.outbox.deadLetterId is required");
            }
            result = store.requeueDeadLetter(id);
            break;
          case "delete-dead":
            result = store.deleteDeadLetters({
              sink,
              failureClass,
              limit,
            });
            break;
          case "purge-delivered":
            result = store.purgeDelivered({
              olderThanMs,
              limit,
            });
            break;
          case "maintenance":
            result = store.maintenance({
              sweep: shell.sweep !== false,
              checkpoint: shell.checkpoint !== false,
              vacuum: shell.vacuum === true,
            });
            break;
          case "check-integrity":
            result = store.checkIntegrity({
              sqlite: true,
              full: shell.full === true,
            });
            break;
          default:
            throw new Error(`Unsupported outbox control action: ${action}`);
        }
        sinkConfig.notifyQueueDepth();

        msg.outbox = shell;
        msg.outbox.result = { action, sink: sink || null, result };
        msg.payload = result;
        node.status(
          action === "status"
            ? result.display
            : {
                fill: result?.ok === false ? "red" : "green",
                shape: "dot",
                text: sink ? `${action} ${sink}` : action,
              }
        );
        send(msg);
        done();
      } catch (error) {
        node.status({
          fill: "red",
          shape: "ring",
          text: "q? · control failed",
        });
        done(error);
      }
    });
  }

  RED.nodes.registerType("durable-outbox-config", DurableOutboxConfigNode);
  RED.nodes.registerType("outbox-sink-config", OutboxSinkConfigNode);
  RED.nodes.registerType("outbox-enqueue", OutboxEnqueueNode);
  RED.nodes.registerType("outbox-claim", OutboxClaimNode);
  RED.nodes.registerType("outbox-settle", OutboxSettleNode);
  RED.nodes.registerType("outbox-control", OutboxControlNode);
};
