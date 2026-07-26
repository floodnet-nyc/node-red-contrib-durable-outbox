# Node-RED durable outbox

A custom Node-RED module that durably queues sink deliveries in local SQLite.
It provides atomic enqueue, deduplication, leased workers, bounded exponential
retry, expired-lease recovery, delivery retention, and dead-letter handling.

The module requires Node.js 22 or newer because it uses the built-in
`node:sqlite` API.

## Nodes

### `durable-outbox-config`

Owns one SQLite database and applies its schema and durability settings. Use a
path on persistent local storage, such as `/data/outbox/outbox.sqlite`.

### `outbox-enqueue`

Reads one job or an array of jobs from `msg.outboxJobs` by default and commits
the complete array in one transaction. It emits the original message only
after the commit succeeds.

```js
msg.outboxJobs = {
    sink: "postgres",
    dedupeKey: `depth:${msg.dev_id}:${msg.timestamp}`,
    payload: msg.payload,
    schemaVersion: 1,
    maxAttempts: 10,
    retryUntilExpired: true,
    maxAgeMs: 7 * 24 * 60 * 60 * 1000,
    baseDelayMs: 2000,
    maxDelayMs: 5 * 60 * 1000
};
return msg;
```

`dedupeKey` is optional. If omitted, the node hashes the sink and canonical
payload. A duplicate enqueue returns the existing job instead of creating
another one.

With `retryUntilExpired: true`, retryable infrastructure failures ignore the
attempt count and remain active until `maxAgeMs`. Backoff is still bounded by
`maxDelayMs`. This is the recommended policy for idempotent PostgreSQL
deliveries.

### `outbox-claim`

Leases the oldest ready job for one sink. Configure an automatic polling
interval, lease duration, and maximum concurrent leases. Any input message
also triggers a poll.

The output has this shape:

```js
{
    payload: { /* persisted job payload */ },
    outbox: {
        id: "...",
        sink: "postgres",
        attempts: 1,
        leaseUntil: 1725038578295
    }
}
```

An expired lease is eligible for recovery by another poll.

After the configured number of consecutive infrastructure failures, claims
for that sink pause for the circuit-breaker cooldown. The next claim after the
cooldown is a half-open probe. A successful delivery closes the circuit.

### `outbox-settle`

Settles `msg.outbox.id`. Configure separate instances for success,
retryable failure, or non-retryable failure. The dynamic mode reads:

```js
msg.outboxOutcome = "success"; // any other value means failure
msg.outboxRetryable = true;
msg.outboxFailureClass = "infrastructure"; // or "data"
msg.outboxError = msg.error;
msg.outboxStatus = 503;
```

The first output receives delivered jobs and scheduled retries. The second
output receives jobs moved to the dead-letter table.

## Tests

Run the complete test suite:

```sh
npm test
```

The tests cover:

- atomic rollback for multi-job enqueue;
- canonical deduplication;
- maximum in-flight enforcement;
- expired lease recovery;
- bounded exponential backoff with jitter;
- successful delivery retention;
- retry exhaustion and non-retryable dead letters;
- dead-letter replay;
- retry-until-expired infrastructure policy;
- per-sink circuit breaking and manual resume;
- filtered bulk dead-letter replay;
- registration and message behavior of all four Node-RED nodes.

## Docker Compose PostgreSQL demo

Build and start Node-RED and PostgreSQL:

```sh
docker compose up --build
```

Open [Node-RED](http://localhost:1880). The demo creates one sensor reading
every two seconds, commits it to SQLite, leases it to a PostgreSQL worker, and
acknowledges it only after the upsert succeeds.

Inspect delivered rows:

```sh
docker compose exec postgres \
  psql -U floodnet -d floodnet_demo \
  -c "SELECT * FROM sensor_readings ORDER BY observed_at DESC LIMIT 10"
```

Demonstrate outage recovery:

```sh
docker compose stop postgres
docker compose logs -f node-red
docker compose start postgres
```

While PostgreSQL is stopped, Node-RED continues committing generated readings
to the `outbox-data` volume, applies bounded backoff, and opens the PostgreSQL
circuit after repeated failures. Once the cooldown expires and PostgreSQL is
available, a successful probe closes the circuit and the worker drains the
pending jobs. These demo jobs retry for up to seven days and do not dead-letter
merely because their attempt count grows.

To inspect the SQLite queue from the host without modifying it:

```sh
docker compose exec node-red node -e \
  'const {DatabaseSync}=require("node:sqlite"); const db=new DatabaseSync("/data/outbox/outbox.sqlite"); console.table(db.prepare("SELECT sink,state,count(*) count FROM outbox_jobs GROUP BY sink,state").all())'
```

## Operations API

The nodes register authenticated Node-RED admin endpoints. For the demo,
`outbox-config` is the configuration-node ID and `postgres-demo` is the sink.

Inspect queue and circuit state:

```sh
curl http://localhost:1880/durable-outbox/outbox-config/status
```

Pause claims manually:

```sh
curl -X POST \
  http://localhost:1880/durable-outbox/outbox-config/sinks/postgres-demo/pause
```

Resume the sink, close its circuit, and make all pending jobs immediately
eligible:

```sh
curl -X POST \
  http://localhost:1880/durable-outbox/outbox-config/sinks/postgres-demo/resume
```

Make pending work immediately eligible without closing a paused circuit:

```sh
curl -X POST \
  http://localhost:1880/durable-outbox/outbox-config/sinks/postgres-demo/retry-now
```

Bulk-requeue dead letters that represent expired PostgreSQL infrastructure
failures:

```sh
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"sink":"postgres-demo","failureClass":"infrastructure","limit":1000}' \
  http://localhost:1880/durable-outbox/outbox-config/dead-letters/requeue
```

When Node-RED authentication is enabled, these routes require
`durable-outbox.read` or `durable-outbox.write` permission.

## Operational notes

- Mount the SQLite directory on persistent local storage. Do not place the WAL
  database on NFS.
- Monitor pending count, oldest pending age, dead-letter count, and disk usage.
- A worker lease should exceed the sink's connection and request timeout.
- Retained delivered records need a periodic retention policy in a production
  deployment.
- The outbox gives PostgreSQL effectively-once behavior because the demo uses
  an idempotent upsert. External HTTP sinks still need an idempotency key to
  avoid duplicates after an ambiguous timeout.
