"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const registerNodes = require("../../nodes/outbox");

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

module.exports = { createHarness };
