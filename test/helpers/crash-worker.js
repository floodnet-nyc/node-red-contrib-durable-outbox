"use strict";

const { OutboxStore } = require("../../lib/outbox-store");

const [filename, sink, batchSize, leaseMs] = process.argv.slice(2);
const store = new OutboxStore(filename);
const jobs = store.claimBatch({
  sink,
  batchSize: Number(batchSize),
  maxInFlight: Number(batchSize),
  leaseMs: Number(leaseMs),
});

if (process.send) process.send({ leased: jobs.length });

// Keep the process alive with outstanding leases until the parent terminates it.
setInterval(() => {}, 60_000);
