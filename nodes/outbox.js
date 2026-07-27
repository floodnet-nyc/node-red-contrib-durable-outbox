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

  function DurableOutboxConfigNode(config) {
    RED.nodes.createNode(this, config);
    this.filename = config.filename;
    const storeOptions = {
      maxQueuedJobs: config.maxQueuedJobs,
      maxJobBytes: megabytesToBytes(config.maxJobMb, 1),
      maxEnqueueBatch: config.maxEnqueueBatch,
      maxDatabaseBytes: megabytesToBytes(config.maxDatabaseMb, 1_024),
    };
    try {
      this.store = new OutboxStore(this.filename, storeOptions);
    } catch (error) {
      this.error(`Unable to open durable outbox: ${error.message}`);
      throw error;
    }

    this.on("close", (removed, done) => {
      try {
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
    this.retryUntilExpired =
      config.retryUntilExpired === true ||
      config.retryUntilExpired === "true";
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
    const optionsProperty = config.optionsProperty || "outbox";
    const unsubscribeQueueDepth = sinkConfig.subscribeQueueDepth((count) => {
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
        const options = optionsProperty
          ? RED.util.getMessageProperty(msg, optionsProperty)
          : undefined;
        if (
          options != null &&
          (typeof options !== "object" || Array.isArray(options))
        ) {
          throw new TypeError(
            `msg.${optionsProperty} must be an object when provided`
          );
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
        };
        send(msg);
        done();
      } catch (error) {
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
        if (jobs.length) {
          node.status({
            fill: "blue",
            shape: "dot",
            text: `${jobs.length} leased`,
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

    function settleOutbox(outbox, outcome, circuitFailure) {
      return sinkConfig.store.settle(outbox.id, {
        leaseToken: outbox.leaseToken,
        success: outcome.success,
        retryable: outcome.retryable,
        error: outcome.error,
        status: outcome.status,
        failureClass: outcome.failureClass,
        circuitFailure,
        circuitBreakerThreshold: sinkConfig.circuitBreakerThreshold,
        circuitBreakerCooldownMs: sinkConfig.circuitBreakerCooldownMs,
      });
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

          const entries = [];
          let circuitFailurePending =
            !outcome.success &&
            sinkConfig.circuitBreakerEnabled &&
            outcome.circuitFailure;
          for (const lease of batch) {
            const result = settleOutbox(
              lease,
              outcome,
              circuitFailurePending
            );
            if (
              circuitFailurePending &&
              result.state !== "stale_lease"
            ) {
              circuitFailurePending = false;
            }
            entries.push({ lease, result });
          }
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
          case "status":
            result = store.stats();
            break;
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
              checkpoint: shell.checkpoint !== false,
              vacuum: shell.vacuum === true,
            });
            break;
          default:
            throw new Error(`Unsupported outbox control action: ${action}`);
        }
        sinkConfig.notifyQueueDepth();

        msg.outbox = shell;
        msg.outbox.result = { action, sink: sink || null, result };
        msg.payload = result;
        node.status({
          fill: "green",
          shape: "dot",
          text: sink ? `${action} ${sink}` : action,
        });
        send(msg);
        done();
      } catch (error) {
        node.status({ fill: "red", shape: "ring", text: "control failed" });
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
