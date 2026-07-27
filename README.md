# Node-RED durable outbox

A custom Node-RED module that durably queues sink deliveries in local SQLite.
It provides atomic enqueue, deduplication, leased workers, bounded exponential
retry, expired-lease recovery, delivery retention, and dead-letter handling.

The module requires Node.js 22 or newer because it uses the built-in
`node:sqlite` API.

## Quickstart

Install the module and create three configuration nodes, then wire four nodes:

```
[Data Source] → [outbox-enqueue] → (original message emitted)
[Inject 5s]  → [outbox-claim]  → [Your Deliver Node] → [outbox-settle (success)]
[Catch]      → [Function]      → [outbox-settle (msg mode)]
```

1. **`durable-outbox-config`** — Set the database path, e.g. `/data/outbox/outbox.sqlite`.
2. **`outbox-sink-config`** — Point to the config above, set a `Sink key` (e.g. `postgres-primary`), enable `retry-until-expired` for idempotent sinks.
3. **`outbox-enqueue`** — Select the sink config. Wire your data source into the input.
4. **`outbox-claim`** — Select the same sink. Feed it a repeating Inject node (e.g. every 5s).
5. **`outbox-settle`** — Select the same sink, set `Outcome: Success`. Wire the claim output through your delivery node (e.g. a PostgreSQL upsert), then into settle.
6. On failure: add a Catch node scoped to your delivery node, wire through a Function node that sets `msg.outboxOutcome = "failure"` and `msg.outboxRetryable = true`, then into a second `outbox-settle (msg mode)`.

That's it. See the [demo flows](#docker-compose-postgresql-demo) below for a complete working example with failure classification and control operations.

## Nodes

### `durable-outbox-config`

Owns one SQLite database and applies its schema and durability settings. Use a
path on persistent local storage, such as `/data/outbox/outbox.sqlite`.

The config node also bounds queue depth, encoded job size, and enqueue batch
size. Its database-size threshold is reported as a health warning; it does not
silently discard or reject already queued work. Job-size and database-warning
settings are configured in MB, where one MB is 1,048,576 bytes.

### `outbox-sink-config`

Defines one logical delivery destination and references its durable outbox.
Its editor groups the policy into:

- **Identity:** durable outbox and stable persisted sink key;
- **Worker capacity:** lease timeout, maximum in-flight jobs, and claim batch size;
- **Retry lifecycle:** bounded-attempt or maximum-age mode, age, and backoff defaults;
- **Circuit breaker:** enablement, failure threshold, and cooldown.

The SQLite job stores the configured sink key, such as `postgres-primary`, not
the generated Node-RED config-node ID.

### `outbox-enqueue`

Persists `msg.payload` as one durable job and emits the original message only
after the commit succeeds. The payload property is configurable.

```js
msg.payload = {
    device_id: msg.dev_id,
    observed_at: msg.timestamp,
    value_mm: msg.value
};
msg.outboxJob = {
    dedupeKey: `depth:${msg.dev_id}:${msg.timestamp}`
};
return msg;
```

The selected sink config supplies the outbox, stable sink key, and retry
defaults. Optional metadata and per-job overrides come from `msg.outboxJob` by
default. Providing a different sink is rejected. Use a separate enqueue node
and sink config for each logical destination.

An array in `msg.payload` is stored as one job payload. Use a standard Split
node before enqueue when each element should be persisted independently.

`dedupeKey` is optional. If omitted, the node hashes the resolved sink and
canonical payload. A duplicate enqueue returns the existing job instead of
creating another one. JSON values, `Date`, and `Buffer` instances are preserved
using a versioned encoding. Unsafe values such as circular references,
functions, streams, and arbitrary class instances are rejected before the
transaction commits.

With `retryUntilExpired: true`, retryable failures ignore the
attempt count and remain active until `maxAgeMs`. Backoff is still bounded by
`maxDelayMs`. This is the recommended policy for idempotent PostgreSQL
deliveries.

### `outbox-claim`

Each input message leases a bounded batch of the oldest ready jobs for one
sink. Lease duration, claim batch, and maximum concurrent leases come from the
selected sink config. Use a standard repeating Inject node to set the polling
interval and optionally trigger once when flows start. A poll fills only the
remaining in-flight capacity.

Individual output mode emits one message per lease:

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

Batch output mode emits one message for the complete claim:

```js
{
    payload: [
        { /* first persisted payload */ },
        { /* second persisted payload */ }
    ],
    outboxBatch: [
        { id: "...", sink: "postgres", leaseToken: "...", attempts: 1 },
        { id: "...", sink: "postgres", leaseToken: "...", attempts: 1 }
    ]
}
```

The payload array can pass through one atomic PostgreSQL batch statement.
`outboxBatch` preserves every corresponding lease and is passed unchanged to
`outbox-settle`; no Batch, Join, packing, or fan-out nodes are required.

An expired lease is eligible for recovery by another poll. Each new claim gets
a fresh lease token. Settlement requires that token, so a slow worker cannot
settle a job after another worker has reclaimed it.

After the configured number of consecutive circuit failures, claims
for that sink pause for the circuit-breaker cooldown. Claims become eligible
again after the cooldown, and a successful delivery closes the circuit.

### `outbox-settle`

Settles `msg.outbox.id` using the selected sink config. A message whose durable
sink key does not match that config is rejected. Configure separate instances
for success, retryable failure, or non-retryable failure. The dynamic mode
reads:

```js
msg.outboxOutcome = "success"; // any other value means failure
msg.outboxRetryable = true;
msg.outboxCircuitFailure = true;
msg.outboxFailureClass = "postgres-connectivity";
msg.outboxError = msg.error;
msg.outboxStatus = 503;
```

`outboxRetryable` controls the individual job. `outboxCircuitFailure` controls
whether this failed attempt increments the sink circuit breaker.
`outboxFailureClass` is an optional operational label used for dead-letter
inspection and recovery filters. When `outboxCircuitFailure` is omitted, the
node preserves the earlier behavior: an explicit `infrastructure` class affects
the circuit, an explicit other class does not, and an unclassified retryable
failure affects the circuit.

The first output receives delivered jobs and scheduled retries. The second
output receives jobs moved to the dead-letter table. A stale settlement is
returned with `msg.outboxSettlement.state === "stale_lease"` and does not
change the job or circuit state.

When `msg.outboxBatch` is present, every preserved lease is settled. A failed
database batch contributes at most one circuit failure, while retry and
dead-letter decisions remain per job. `msg.outboxSettlements` groups the
results into `delivered`, `retrying`, `dead`, and `stale` arrays. If a batch
contains both retrying and dead jobs, the first output receives the active
lease subset and the second receives the dead-letter subset.

### `outbox-control`

Provides message-driven operations inside a normal Node-RED flow. The selected
sink supplies both its stable key and parent outbox, so the node does not need a
second Outbox selector. An action can be fixed in the editor or supplied
dynamically:

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

`status`, `purge-delivered`, and `maintenance` operate on the selected sink's
entire parent outbox. Dead-letter and retry controls default to the selected
sink key; `msg.sink` can override that key within the same outbox.

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
- native batch messages and batch-aware settlement;
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
- registration and message behavior of all six Node-RED node types.

## Docker Compose PostgreSQL demo

Build and start Node-RED and PostgreSQL:

```sh
docker compose up --build
```

Open [Node-RED](http://localhost:1880). The demo creates one sensor reading
every two seconds, commits it to SQLite, leases it to a PostgreSQL worker, and
acknowledges it only after the upsert succeeds. Delivery uses
`node-red-contrib-postgresql` 0.15.4 with an environment-configured connection
pool and a parameterized `msg.query` / `msg.params` upsert. The
`Durable PostgreSQL outbox demo` tab shows individual delivery. The
`Batch outbox` tab claims one payload array with its `outboxBatch` leases,
executes one PostgreSQL `unnest` upsert, and passes the batch directly to
batch-aware settlement.

The host `./demo` directory is mounted at `/data`, so deploying in the Node-RED
editor writes directly to `./demo/flows.json` and changes survive container
restarts. Mounting the containing directory is intentional: Node-RED saves by
writing a temporary file and atomically renaming it over `flows.json`, which
cannot work when `flows.json` itself is an individual Docker bind mount. The
SQLite outbox remains separately persisted in `./data/outbox`.

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
