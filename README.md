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

The config node also bounds queue depth, encoded job size, and enqueue batch
size. Its database-size threshold is reported as a health warning; it does not
silently discard or reject already queued work.

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
another one. JSON values, `Date`, and `Buffer` instances are preserved using a
versioned encoding. Unsafe values such as circular references, functions,
streams, and arbitrary class instances are rejected before the transaction
commits.

With `retryUntilExpired: true`, retryable infrastructure failures ignore the
attempt count and remain active until `maxAgeMs`. Backoff is still bounded by
`maxDelayMs`. This is the recommended policy for idempotent PostgreSQL
deliveries.

### `outbox-claim`

Leases a bounded batch of the oldest ready jobs for one sink. Configure an
automatic polling interval, lease duration, claim batch, and maximum concurrent
leases. A poll fills only the remaining in-flight capacity. Any input message
also triggers a poll.

The output has this shape:

```js
{
    payload: { /* persisted job payload */ },
    outbox: {
        id: "...",
        sink: "postgres",
        attempts: 1,
        leaseUntil: 1725038578295,
        leaseToken: "..."
    }
}
```

An expired lease is eligible for recovery by another poll. Each new claim gets
a fresh lease token. Settlement requires that token, so a slow worker cannot
settle a job after another worker has reclaimed it.

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
output receives jobs moved to the dead-letter table. A stale settlement is
returned with `msg.outboxSettlement.state === "stale_lease"` and does not
change the job or circuit state.

### `outbox-control`

Provides message-driven operations inside a normal Node-RED flow. An action
can be fixed in the editor or supplied dynamically:

```js
msg.outboxAction = "resume";
msg.sink = "postgres";
return msg;
```

Supported actions are:

- `status`, `pause`, `resume`, and `retry-now`;
- `list-dead`, `requeue-dead`, and `requeue-one`;
- `delete-dead` for reviewed, bounded dead-letter deletion;
- `purge-delivered` for bounded retention cleanup;
- `maintenance` for WAL checkpointing and an explicitly requested vacuum.

`resume` closes the sink circuit and makes all pending work immediately
eligible. Results are returned in both `msg.payload` and
`msg.outboxControl.result`.

## Tests

Run the complete test suite:

```sh
npm test
```

The tests cover:

- atomic rollback for multi-job enqueue;
- canonical deduplication;
- `Date`/`Buffer` serialization and unsafe-payload rejection;
- maximum in-flight enforcement;
- bounded batch claiming;
- expired lease recovery;
- stale-worker lease fencing;
- bounded exponential backoff with jitter;
- successful delivery retention;
- retry exhaustion and non-retryable dead letters;
- dead-letter replay;
- retry-until-expired infrastructure policy;
- per-sink circuit breaking and manual resume;
- filtered bulk dead-letter replay;
- SQL-filtered dead-letter listing;
- bounded retention, deletion, health, and capacity controls;
- registration and message behavior of all five Node-RED nodes.

## Docker Compose PostgreSQL demo

Build and start Node-RED and PostgreSQL:

```sh
docker compose up --build
```

Open [Node-RED](http://localhost:1880). The demo creates one sensor reading
every two seconds, commits it to SQLite, leases it to a PostgreSQL worker, and
acknowledges it only after the upsert succeeds. Delivery uses
`node-red-contrib-postgresql` 0.15.4 with an environment-configured connection
pool and a parameterized `msg.query` / `msg.params` upsert.

Node-RED editor state is stored in the Compose-managed `node-red-data` volume,
so Deploy can atomically replace `flows.json` and changes survive container
restarts. The SQLite outbox remains separately persisted in
`./data/outbox`. To discard editor changes and restore the packaged demo flow,
remove only the `node-red-data` volume and recreate the service:

```sh
docker compose down
docker volume rm floodnet-ingest-queue_node-red-data
docker compose up --build
```

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

## Message-driven operations

The demo includes visible Inject → `outbox-control` → Debug flows for:

- inspecting queue and circuit state;
- resuming PostgreSQL and immediately draining pending work;
- bulk-requeueing expired PostgreSQL infrastructure failures.

The same node can be driven dynamically by a Dashboard button, MQTT command,
authenticated HTTP-In flow, or another operational workflow:

```js
msg.outboxAction = "requeue-dead";
msg.sink = "postgres-demo";
msg.failureClass = "infrastructure";
msg.limit = 1000;
return msg;
```

To replay one reviewed dead letter:

```js
msg.outboxAction = "requeue-one";
msg.deadLetterId = "the-job-id";
return msg;
```

To purge at most 1,000 delivered records older than one day:

```js
msg.outboxAction = "purge-delivered";
msg.olderThanMs = 24 * 60 * 60 * 1000;
msg.limit = 1000;
return msg;
```

After cleanup, checkpoint the WAL. Vacuum is opt-in because it can block
ingestion:

```js
msg.outboxAction = "maintenance";
msg.vacuum = false;
return msg;
```

There are no built-in HTTP management endpoints. Authentication, authorization,
auditing, and operator presentation remain part of the surrounding Node-RED
flow.

## Operational notes

- Mount the SQLite directory on persistent local storage. Do not place the WAL
  database on NFS.
- Monitor `status` health fields for queue depth, oldest queued age, expired
  leases, dead-letter count, and database/WAL size.
- A worker lease should exceed the sink's connection and request timeout.
- Trigger `purge-delivered` periodically in bounded batches. Run an optional
  maintenance vacuum only during a quiet window.
- The outbox gives PostgreSQL effectively-once behavior because the demo uses
  an idempotent upsert. External HTTP sinks still need an idempotency key to
  avoid duplicates after an ambiguous timeout.
