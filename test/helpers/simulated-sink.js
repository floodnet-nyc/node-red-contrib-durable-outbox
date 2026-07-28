"use strict";

const { setTimeout: delay } = require("node:timers/promises");

class SimulatedSink {
  constructor(options = {}) {
    this.mode = options.mode || "healthy";
    this.latencyMs = Math.max(0, Number(options.latencyMs) || 0);
    this.jitterMs = Math.max(0, Number(options.jitterMs) || 0);
    this.failureRate = Math.max(
      0,
      Math.min(1, Number(options.failureRate) || 0)
    );
    this.random = options.random || Math.random;
    this.retryable = options.retryable !== false;
    this.circuitFailure = options.circuitFailure !== false;
    this.failureClass = options.failureClass || "simulated-outage";
    this.active = 0;
    this.maxActive = 0;
    this.attempts = 0;
    this.failedAttempts = 0;
    this.successfulAttempts = 0;
    this.successfulJobIds = new Set();
    this.successfulJobDeliveries = new Map();
  }

  setMode(mode) {
    this.mode = mode;
  }

  leases(message) {
    return message.outbox?.batch || (message.outbox?.id ? [message.outbox] : []);
  }

  async process(message) {
    this.active += 1;
    this.maxActive = Math.max(this.maxActive, this.active);
    this.attempts += 1;
    try {
      const jitter = this.jitterMs
        ? Math.floor(this.random() * (this.jitterMs + 1))
        : 0;
      if (this.latencyMs + jitter) {
        await delay(this.latencyMs + jitter);
      } else {
        await new Promise((resolve) => setImmediate(resolve));
      }

      const failed =
        this.mode === "outage" ||
        (this.mode === "intermittent" && this.random() < this.failureRate);
      if (failed) {
        this.failedAttempts += 1;
        message.outbox.retryable = this.retryable;
        message.outbox.circuitFailure = this.circuitFailure;
        message.outbox.failureClass = this.failureClass;
        message.outbox.error = new Error("Simulated delivery outage");
      } else {
        this.successfulAttempts += 1;
        message.outbox.outcome = "success";
        delete message.outbox.retryable;
        delete message.outbox.circuitFailure;
        delete message.outbox.failureClass;
        delete message.outbox.error;
        for (const lease of this.leases(message)) {
          this.successfulJobIds.add(lease.id);
          this.successfulJobDeliveries.set(
            lease.id,
            (this.successfulJobDeliveries.get(lease.id) || 0) + 1
          );
        }
      }
      return message;
    } finally {
      this.active -= 1;
    }
  }
}

module.exports = { SimulatedSink };
