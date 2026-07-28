"use strict";

const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const { setTimeout: delay } = require("node:timers/promises");
const test = require("node:test");

function compose(...args) {
  return execFileSync("docker", ["compose", ...args], {
    cwd: process.cwd(),
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function readingCount() {
  const output = compose(
    "exec",
    "-T",
    "postgres",
    "psql",
    "-U",
    "floodnet",
    "-d",
    "floodnet_demo",
    "-Atc",
    "SELECT COUNT(*) FROM sensor_readings WHERE device_id = 'demo-sensor-1';"
  );
  return Number(output);
}

test(
  "the Compose demo delivers through node-red-contrib-postgresql",
  { timeout: 30_000 },
  async () => {
    assert.ok(
      compose("ps", "-q", "postgres"),
      "PostgreSQL is not running; run docker compose up --build -d --wait first"
    );
    assert.ok(
      compose("ps", "-q", "node-red"),
      "Node-RED is not running; run docker compose up --build -d --wait first"
    );

    const before = readingCount();
    let after = before;
    const deadline = Date.now() + 15_000;
    while (after <= before && Date.now() < deadline) {
      await delay(1_000);
      after = readingCount();
    }

    process.stdout.write(
      `INTEGRATION_METRICS ${JSON.stringify({
        sensorReadingsBefore: before,
        sensorReadingsAfter: after,
      })}\n`
    );
    assert.ok(
      after > before,
      "the demo did not deliver a new PostgreSQL reading within 15 seconds"
    );
  }
);
