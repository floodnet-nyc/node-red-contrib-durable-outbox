"use strict";

const { OutboxStore } = require("../lib/outbox-store");

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
    this.leaseMs = positiveNumber(config.leaseMs, 60_000);
    this.maxInFlight = positiveNumber(config.maxInFlight, 1);
    this.batchSize = positiveNumber(config.batchSize, 10);
    this.maxAttempts = positiveNumber(config.maxAttempts, 10);
    this.retryUntilExpired =
      config.retryUntilExpired === true ||
      config.retryUntilExpired === "true";
    this.maxAgeMs = positiveNumber(config.maxAgeMs, 86_400_000);
    this.baseDelayMs = positiveNumber(config.baseDelayMs, 2_000);
    this.maxDelayMs = positiveNumber(config.maxDelayMs, 300_000);
    this.circuitBreakerEnabled =
      config.circuitBreakerEnabled !== false &&
      config.circuitBreakerEnabled !== "false";
    this.circuitBreakerThreshold = positiveNumber(
      config.circuitBreakerThreshold,
      3
    );
    this.circuitBreakerCooldownMs = positiveNumber(
      config.circuitBreakerCooldownMs,
      30_000
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
    const optionsProperty = config.optionsProperty || "outboxJob";
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
        msg.outboxEnqueue = {
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
              outboxBatch: jobs.map((job) => {
                const { payload, ...metadata } = job;
                return metadata;
              }),
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
      const success = configuredOutcome === "success"
        || (configuredOutcome === "msg" && msg.outboxOutcome === "success");

      let retryable;
      if (configuredOutcome === "retry") {
        retryable = true;
      } else if (configuredOutcome === "dead") {
        retryable = false;
      } else {
        retryable = msg.outboxRetryable !== false;
      }

      const error =
        msg.outboxError ||
        msg.error ||
        (success ? null : msg.payload?.error) ||
        (success ? null : "Delivery failed");
      const status =
        msg.statusCode ?? msg.outboxStatus ?? msg.payload?.statusCode;
      const failureClass =
        msg.outboxFailureClass ||
        (retryable ? "infrastructure" : "data");

      let circuitFailure = false;
      if (!success) {
        if (
          msg.outboxCircuitFailure != null &&
          typeof msg.outboxCircuitFailure !== "boolean"
        ) {
          throw new TypeError("msg.outboxCircuitFailure must be a boolean");
        }
        circuitFailure =
          msg.outboxCircuitFailure ??
          (msg.outboxFailureClass
            ? failureClass === "infrastructure"
            : retryable);
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
        if (Array.isArray(msg.outboxBatch)) {
          if (!msg.outboxBatch.length) {
            throw new Error("msg.outboxBatch must contain at least one lease");
          }
          for (const outbox of msg.outboxBatch) validateOutbox(outbox);

          const entries = [];
          let circuitFailurePending =
            !outcome.success &&
            sinkConfig.circuitBreakerEnabled &&
            outcome.circuitFailure;
          for (const outbox of msg.outboxBatch) {
            const result = settleOutbox(
              outbox,
              outcome,
              circuitFailurePending
            );
            if (
              circuitFailurePending &&
              result.state !== "stale_lease"
            ) {
              circuitFailurePending = false;
            }
            entries.push({ outbox, result });
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
            outboxBatch: selected.map(({ outbox }) => outbox),
            outboxSettlements: summary,
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
        msg.outboxSettlement = result;

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
        const action = msg.outboxAction || config.action || "status";
        const sink = msg.sink || sinkConfig.sinkKey;
        const failureClass =
          msg.failureClass || config.failureClass || undefined;
        const limit = Number(msg.limit ?? config.limit) || 100;
        const requestedAge = Number(
          msg.olderThanMs ?? config.olderThanMs ?? 86_400_000
        );
        const olderThanMs = Number.isFinite(requestedAge)
          ? requestedAge
          : 86_400_000;
        const id = msg.deadLetterId || msg.outbox?.id || msg.id;
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
              throw new Error(
                "msg.deadLetterId, msg.outbox.id, or msg.id is required"
              );
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
              checkpoint: msg.checkpoint !== false,
              vacuum: msg.vacuum === true,
            });
            break;
          default:
            throw new Error(`Unsupported outbox control action: ${action}`);
        }
        sinkConfig.notifyQueueDepth();

        msg.outboxControl = {
          action,
          sink: sink || null,
          result,
        };
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
