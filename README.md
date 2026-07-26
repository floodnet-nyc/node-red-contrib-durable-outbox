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
    maxAgeMs: 24 * 60 * 60 * 1000,
    baseDelayMs: 2000,
    maxDelayMs: 5 * 60 * 1000
};
return msg;
```

`dedupeKey` is optional. If omitted, the node hashes the sink and canonical
payload. A duplicate enqueue returns the existing job instead of creating
another one.

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

### `outbox-settle`

Settles `msg.outbox.id`. Configure separate instances for success,
retryable failure, or non-retryable failure. The dynamic mode reads:

```js
msg.outboxOutcome = "success"; // any other value means failure
msg.outboxRetryable = true;
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
to the `outbox-data` volume and schedules bounded retries. Once PostgreSQL is
available, the worker drains the pending jobs.

To inspect the SQLite queue from the host without modifying it:

```sh
docker compose exec node-red node -e \
  'const {DatabaseSync}=require("node:sqlite"); const db=new DatabaseSync("/data/outbox/outbox.sqlite"); console.table(db.prepare("SELECT sink,state,count(*) count FROM outbox_jobs GROUP BY sink,state").all())'
```

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
