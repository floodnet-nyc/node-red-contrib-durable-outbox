"use strict";

const { OutboxStore } = require("../lib/outbox-store");

module.exports = function registerOutboxNodes(RED) {
  function DurableOutboxConfigNode(config) {
    RED.nodes.createNode(this, config);
    this.filename = config.filename;
    this.circuitBreakerThreshold =
      Number(config.circuitBreakerThreshold) || 3;
    this.circuitBreakerCooldownMs =
      Number(config.circuitBreakerCooldownMs) || 30_000;
    const storeOptions = {
      maxQueuedJobs: config.maxQueuedJobs,
      maxJobBytes: config.maxJobBytes,
      maxEnqueueBatch: config.maxEnqueueBatch,
      maxDatabaseBytes: config.maxDatabaseBytes,
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

  function getConfigNode(runtimeNode, id) {
    const configNode = RED.nodes.getNode(id);
    if (!configNode?.store) {
      runtimeNode.status({ fill: "red", shape: "ring", text: "not configured" });
      throw new Error("A valid durable outbox configuration is required");
    }
    return configNode;
  }

  function OutboxEnqueueNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const configNode = getConfigNode(node, config.outbox);
    const jobProperty = config.jobProperty || "outboxJobs";

    node.on("input", (msg, send, done) => {
      send = send || node.send.bind(node);
      try {
        const jobs = RED.util.getMessageProperty(msg, jobProperty);
        if (jobs == null) {
          throw new Error(`msg.${jobProperty} does not contain an outbox job`);
        }
        const results = configNode.store.enqueue(jobs);
        msg.outboxEnqueue = {
          jobs: results,
          inserted: results.filter((job) => job.inserted).length,
          duplicates: results.filter((job) => !job.inserted).length,
        };
        node.status({
          fill: "green",
          shape: "dot",
          text: `${msg.outboxEnqueue.inserted} queued`,
        });
        send(msg);
        done();
      } catch (error) {
        node.status({ fill: "red", shape: "ring", text: "enqueue failed" });
        done(error);
      }
    });
  }

  function OutboxClaimNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const configNode = getConfigNode(node, config.outbox);
    const sink = config.sink;
    const leaseMs = Number(config.leaseMs) || 60_000;
    const maxInFlight = Number(config.maxInFlight) || 1;
    const batchSize = Number(config.batchSize) || 10;

    function claim(send, done) {
      try {
        const jobs = configNode.store.claimBatch({
          sink,
          leaseMs,
          maxInFlight,
          batchSize,
        });
        if (jobs.length) {
          node.status({
            fill: "blue",
            shape: "dot",
            text: `${jobs.length} leased`,
          });
          for (const job of jobs) {
            send({
              _msgid: RED.util.generateId(),
              payload: job.payload,
              outbox: job,
            });
          }
        } else {
          const control = configNode.store.getSinkControl(sink);
          if (control.manuallyPaused) {
            node.status({ fill: "yellow", shape: "dot", text: "paused" });
          } else if (
            control.pausedUntil != null &&
            control.pausedUntil > Date.now()
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
    const configNode = getConfigNode(node, config.outbox);
    const configuredOutcome = config.outcome || "success";

    node.on("input", (msg, send, done) => {
      send = send || node.send.bind(node);
      try {
        const id = msg.outbox?.id;
        if (!id) throw new Error("msg.outbox.id is required to settle a job");

        const success =
          configuredOutcome === "success" ||
          (configuredOutcome === "msg" && msg.outboxOutcome === "success");
        const retryable =
          configuredOutcome === "retry"
            ? true
            : configuredOutcome === "dead"
              ? false
              : msg.outboxRetryable !== false;
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

        const result = configNode.store.settle(id, {
          leaseToken: msg.outbox.leaseToken,
          success,
          retryable,
          error,
          status,
          failureClass,
          circuitBreakerThreshold: configNode.circuitBreakerThreshold,
          circuitBreakerCooldownMs: configNode.circuitBreakerCooldownMs,
        });
        msg.outboxSettlement = result;

        if (result.state === "stale_lease") {
          node.status({ fill: "yellow", shape: "ring", text: "stale lease" });
          send([msg, null]);
        } else if (result.state === "dead") {
          node.status({ fill: "red", shape: "dot", text: "dead letter" });
          send([null, msg]);
        } else if (result.state === "delivered") {
          node.status({ fill: "green", shape: "dot", text: "delivered" });
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
    const configNode = getConfigNode(node, config.outbox);

    node.on("input", (msg, send, done) => {
      send = send || node.send.bind(node);
      try {
        const action = msg.outboxAction || config.action || "status";
        const sink = msg.sink || config.sink || undefined;
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
            result = configNode.store.stats();
            break;
          case "pause":
            if (!sink) throw new Error("A sink is required to pause");
            result = configNode.store.pauseSink(sink);
            break;
          case "resume":
            if (!sink) throw new Error("A sink is required to resume");
            result = configNode.store.resumeSink(sink, { retryNow: true });
            break;
          case "retry-now":
            if (!sink) throw new Error("A sink is required to retry jobs");
            result = configNode.store.retryNow(sink);
            break;
          case "list-dead": {
            const rows = configNode.store.listDeadLetters({
              sink,
              failureClass,
              limit,
            });
            result = { jobs: rows, count: rows.length };
            break;
          }
          case "requeue-dead":
            result = configNode.store.requeueDeadLetters({
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
            result = configNode.store.requeueDeadLetter(id);
            break;
          case "delete-dead":
            result = configNode.store.deleteDeadLetters({
              sink,
              failureClass,
              limit,
            });
            break;
          case "purge-delivered":
            result = configNode.store.purgeDelivered({
              olderThanMs,
              limit,
            });
            break;
          case "maintenance":
            result = configNode.store.maintenance({
              checkpoint: msg.checkpoint !== false,
              vacuum: msg.vacuum === true,
            });
            break;
          default:
            throw new Error(`Unsupported outbox control action: ${action}`);
        }

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
  RED.nodes.registerType("outbox-enqueue", OutboxEnqueueNode);
  RED.nodes.registerType("outbox-claim", OutboxClaimNode);
  RED.nodes.registerType("outbox-settle", OutboxSettleNode);
  RED.nodes.registerType("outbox-control", OutboxControlNode);
};
