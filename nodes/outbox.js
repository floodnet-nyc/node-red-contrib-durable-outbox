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
    try {
      this.store = new OutboxStore(this.filename);
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
    const intervalMs = Math.max(0, Number(config.intervalMs) || 0);
    let timer = null;

    function claim(send, done) {
      try {
        const job = configNode.store.claim({ sink, leaseMs, maxInFlight });
        if (job) {
          node.status({
            fill: "blue",
            shape: "dot",
            text: `leased attempt ${job.attempts}`,
          });
          send({
            _msgid: RED.util.generateId(),
            payload: job.payload,
            outbox: job,
          });
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

    if (intervalMs > 0) {
      timer = setInterval(() => claim(node.send.bind(node)), intervalMs);
      timer.unref?.();
      setImmediate(() => claim(node.send.bind(node)));
    }

    node.on("close", (removed, done) => {
      if (timer) clearInterval(timer);
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
          success,
          retryable,
          error,
          status,
          failureClass,
          circuitBreakerThreshold: configNode.circuitBreakerThreshold,
          circuitBreakerCooldownMs: configNode.circuitBreakerCooldownMs,
        });
        msg.outboxSettlement = result;

        if (result.state === "dead") {
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

  RED.nodes.registerType("durable-outbox-config", DurableOutboxConfigNode);
  RED.nodes.registerType("outbox-enqueue", OutboxEnqueueNode);
  RED.nodes.registerType("outbox-claim", OutboxClaimNode);
  RED.nodes.registerType("outbox-settle", OutboxSettleNode);

  if (RED.httpAdmin) {
    const readPermission = RED.auth?.needsPermission
      ? RED.auth.needsPermission("durable-outbox.read")
      : (request, response, next) => next();
    const writePermission = RED.auth?.needsPermission
      ? RED.auth.needsPermission("durable-outbox.write")
      : (request, response, next) => next();

    const getStore = (request, response) => {
      const configNode = RED.nodes.getNode(request.params.id);
      if (!configNode?.store) {
        response.status(404).json({ error: "Outbox configuration not found" });
        return null;
      }
      return configNode.store;
    };

    RED.httpAdmin.get(
      "/durable-outbox/:id/status",
      readPermission,
      (request, response) => {
        const store = getStore(request, response);
        if (store) response.json(store.stats());
      }
    );

    RED.httpAdmin.post(
      "/durable-outbox/:id/sinks/:sink/pause",
      writePermission,
      (request, response) => {
        const store = getStore(request, response);
        if (store) response.json(store.pauseSink(request.params.sink));
      }
    );

    RED.httpAdmin.post(
      "/durable-outbox/:id/sinks/:sink/resume",
      writePermission,
      (request, response) => {
        const store = getStore(request, response);
        if (store) {
          response.json(
            store.resumeSink(request.params.sink, { retryNow: true })
          );
        }
      }
    );

    RED.httpAdmin.post(
      "/durable-outbox/:id/sinks/:sink/retry-now",
      writePermission,
      (request, response) => {
        const store = getStore(request, response);
        if (store) response.json(store.retryNow(request.params.sink));
      }
    );

    RED.httpAdmin.post(
      "/durable-outbox/:id/dead-letters/requeue",
      writePermission,
      (request, response) => {
        const store = getStore(request, response);
        if (store) {
          response.json(
            store.requeueDeadLetters({
              sink: request.body?.sink,
              failureClass: request.body?.failureClass,
              limit: request.body?.limit,
            })
          );
        }
      }
    );
  }
};
