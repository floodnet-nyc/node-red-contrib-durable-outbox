# Node-RED durable outbox

A custom Node-RED module that durably queues sink deliveries in local SQLite.
It provides atomic enqueue, deduplication, leased workers, bounded exponential
retry, expired-lease recovery, delivery retention, and dead-letter handling.

The module requires Node.js 22 or newer because it uses the built-in
`node:sqlite` API.

## Quickstart

Install the module and create three configuration nodes, then wire four nodes:

```
[Data Source] → [outbox-enqueue] → persisted or duplicate
                              └─→ not persisted → [Alarm / pause source]
[Inject 5s]  → [outbox-claim]  → [Your Deliver Node] → [outbox-settle (Success)]
                               └─ [Catch] → [fn: msg.outbox.retryable=true] ─┘
```

1. **`durable-outbox-config`** — Set the database path, e.g. `/data/outbox/outbox.sqlite`.
2. **`outbox-sink-config`** — Point to the config above, set a `Sink key` (e.g. `postgres-primary`), and leave `retry-until-expired` enabled for idempotent sinks.
3. **`outbox-enqueue`** — Select the sink config. Wire your data source into the input.
4. **`outbox-claim`** — Select the same sink. Feed it a repeating Inject node (e.g. every 5s). Wire through your delivery node (e.g. a PostgreSQL upsert).
5. **`outbox-settle`** — Select the same sink, set `Outcome: Success`. Wire the claim output (after your delivery node) into the settle input. The success path needs no message properties.
6. On failure: add a Catch node scoped to your delivery node, wire through a Function node that sets `msg.outbox.retryable = true`, then into the same settle node. One settle node in `Success` mode handles both paths — the message property overrides the dropdown.

That's it. See the [demo flows](#docker-compose-postgresql-demo) below for a complete working example with failure classification and control operations.

## Nodes

### `durable-outbox-config`

Owns one SQLite database and applies its schema and durability settings. Use a
path on persistent local storage, such as `/data/outbox/outbox.sqlite`.

The config node also bounds queue depth, encoded job size, enqueue batch size,
logical database use, and minimum filesystem free space. Reusable SQLite pages
do not count as database use. Set the free-space reserve to zero only when
another system enforces disk headroom. Size settings use MB, where one MB is
1,048,576 bytes.

Delivered history is retained for 24 hours by default and cleaned automatically
in bounded batches. Cleanup starts normally with expired records. When logical
database use reaches the configured high-water mark, the oldest delivered
records become expendable—even within the normal retention window—until use
reaches the low-water target. Before rejecting an enqueue at the database
boundary, the store makes a final bounded attempt to reclaim delivered history.
Pending, leased, and dead-letter jobs are never removed by this process.

The delivered retention period is therefore a target rather than a guarantee
under storage pressure. This preserves ingestion and unfinished work at the
cost of shortening successful-delivery history and its deduplication window.

Automatic cleanup settings are grouped into:

- **Delivered history:** retention period, cleanup interval, and cleanup batch
  size;
- **Storage pressure:** cleanup-start percentage, cleanup-target percentage,
  and whether delivered history may be evicted early to protect ingestion.

The defaults run at most 10,000 deletions per transaction, start pressure
cleanup at 80%, and continue in yielded batches toward 70%.

### `outbox-sink-config`

Defines one logical delivery destination and references its durable outbox.
Its editor groups the policy into:

- **Identity:** durable outbox and stable persisted sink key;
- **Worker capacity:** lease timeout, maximum in-flight jobs, and claim batch size;
- **Retry lifecycle:** bounded-attempt or maximum-age mode, age, and backoff defaults;
- **Circuit breaker:** enablement, failure threshold, and cooldown.

The defaults lease and claim up to 10 jobs, so the default batch is not
artificially reduced to one job. Retryable failures remain active until the
24-hour maximum age by default; select bounded-attempt mode explicitly when a
sink should stop after 10 delivery attempts.

The SQLite job stores the configured sink key, such as `postgres-primary`, not
the generated Node-RED config-node ID.

### `outbox-enqueue`

Persists `msg.payload` as one durable job and emits the original message only
after the commit succeeds. The payload property is configurable.

The first output emits persisted jobs and duplicates. The second output emits
inputs that were not persisted because a queue, database, or filesystem
capacity boundary remained after automatic cleanup. Wire the second output to
an alarm, upstream pause, or source-specific durable recovery path. Repeated
capacity errors are summarized rather than logged once per input.

```js
msg.payload = {
    device_id: msg.dev_id,
    observed_at: msg.timestamp,
    value_mm: msg.value
};
msg.outbox = {
    dedupeKey: `depth:${msg.dev_id}:${msg.timestamp}`
};
return msg;
```

The selected sink config supplies the outbox, stable sink key, and retry
defaults. Optional metadata and per-job overrides come from `msg.outbox` by
default. Providing a different sink is rejected. Use a separate enqueue node
and sink config for each logical destination.

An array in `msg.payload` is stored as one job payload. Use a standard Split
node before enqueue when each element should be persisted independently.

`dedupeKey` is optional. If omitted, the node hashes the resolved sink and
canonical payload. A duplicate enqueue returns the existing job instead of
creating another one. Job IDs are generated and managed by the outbox; callers
must use `dedupeKey`, not `msg.outbox.id`, for idempotency. JSON values, `Date`,
and `Buffer` instances are preserved
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
    outbox: {
        batch: [
            { id: "...", sink: "postgres", leaseToken: "...", attempts: 1 },
            { id: "...", sink: "postgres", leaseToken: "...", attempts: 1 }
        ]
    }
}
```

The payload array can pass through one atomic PostgreSQL batch statement.
<code>msg.outbox.batch</code> preserves every corresponding lease and is passed unchanged to
`outbox-settle`; no Batch, Join, packing, or fan-out nodes are required.

An expired lease is eligible for recovery by another poll. Each new claim gets
a fresh lease token. Settlement requires that token, so a slow worker cannot
settle a job after another worker has reclaimed it.

A payload that cannot be decoded is quarantined to the dead-letter table with
failure class `outbox-corruption`, at a maximum of ten rows per poll. Healthy
rows in the same claim can continue. These records remain inspectable but
cannot be replayed without external repair. An unknown versioned payload
encoding pauses the sink instead, protecting data that may require a newer
module version.

After the configured number of consecutive circuit failures, claims
for that sink pause for the circuit-breaker cooldown. Claims become eligible
again after the cooldown, and a successful delivery closes the circuit.

### `outbox-settle`

Settles `msg.outbox.id` using the selected sink config. A message whose durable
sink key does not match that config is rejected. Configure separate instances
for success, retryable failure, or non-retryable failure. The configured
outcome is the default, and <code>msg.outbox.retryable</code> always overrides it:

```js
msg.outbox.retryable = true;              // retry with backoff (overrides dropdown)
```

A settle node set to <code>Success</code> handles both paths: the success
wire needs no extra properties; the failure wire only needs
<code>msg.outbox.retryable</code>. Advanced users can override the derived
circuit and failure class with <code>msg.outbox.circuitFailure</code>,
<code>msg.outbox.failureClass</code>, and <code>msg.outbox.error</code>.

The first output receives delivered jobs and scheduled retries. The second
output receives jobs moved to the dead-letter table. A stale settlement is
returned with `msg.outbox.result.state === "stale_lease"` and does not
change the job or circuit state.

When `msg.outbox.batch` is present, every preserved lease is settled. A failed
database batch contributes at most one circuit failure, while retry and
dead-letter decisions remain per job. `msg.outbox.result` groups the
results into `delivered`, `retrying`, `dead`, and `stale` arrays. If a batch
contains both retrying and dead jobs, the first output receives the active
lease subset and the second receives the dead-letter subset.

### `outbox-control`

Provides message-driven operations inside a normal Node-RED flow. The selected
sink supplies both its stable key and parent outbox, so the node does not need a
second Outbox selector. An action can be fixed in the editor or supplied
dynamically:

```js
msg.outbox.action = "resume";
msg.outbox.sink = "postgres";
return msg;
```

Supported actions are:

- `status`, `pause`, `resume`, and `retry-now`;
- `list-dead`, `requeue-dead`, and `requeue-one`;
- `delete-dead` for reviewed, bounded dead-letter deletion;
- `purge-delivered` for bounded retention cleanup;
- `maintenance` for WAL checkpointing and an explicitly requested vacuum;
- `check-integrity` for schema, trigger, queue-counter, and SQLite checks.

`resume` closes the sink circuit and makes all pending work immediately
eligible. Results are returned in both `msg.payload` and
`msg.outbox.result`.

`status`, `purge-delivered`, `maintenance`, and `check-integrity` operate on
the selected sink's entire parent outbox. Dead-letter and retry controls
default to the selected sink key; `msg.outbox.sink` can override that key
within the same outbox. Bulk replay skips `outbox-corruption` records and
reports them as `corruptSkipped`.

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
- managed-ID enforcement and distinct storage admission failures;
- corrupt-payload quarantine and unknown-encoding sink pauses;
- explicit integrity checks and startup queue-counter reconciliation;
- the No-SQL write-plan demo's structure, Function syntax, cross-realm object
  validation, and batch lease partitioning;
- registration and message behavior of all six Node-RED node types.

### Stress and outage tests

The primary stress suite uses an on-disk SQLite outbox and a deterministic
asynchronous sink simulator. It does not require Docker or PostgreSQL:

```sh
npm run test:stress
```

The suite measures growing-backlog enqueue latency, claim/settle drain
throughput, event-loop delay, database/WAL size, and expired-lease cleanup. Its
Node-RED scenario opens the circuit during a simulated outage, verifies that the
entire backlog remains durable, resumes the sink, drains every job exactly once,
and fences a slow response after its lease is reclaimed. A separate process is
killed with outstanding leases to verify WAL integrity and restart recovery.
Metrics are emitted as single-line `STRESS_METRICS` JSON records for collection
by CI.

The default profile uses 5,000 jobs. Larger or environment-specific profiles
can be selected without changing the tests:

```sh
STRESS_JOBS=100000 \
STRESS_EXPIRED_JOBS=100000 \
STRESS_BATCH_SIZE=100 \
STRESS_MAX_IN_FLIGHT=500 \
npm run test:stress
```

Available performance budgets are:

- `STRESS_MIN_JOBS_PER_SECOND` — minimum direct-store enqueue throughput
  (default `100`);
- `STRESS_MAX_EVENT_LOOP_P99_MS` — maximum simulated-flow p99 event-loop delay
  (default `250`);
- `STRESS_MAX_STORE_MS` and `STRESS_MAX_SUITE_MS` — per-test timeout budgets
  (default `120000`).

These deliberately portable defaults catch severe regressions. Record metrics
on the intended production hardware and tighten the budgets there.

PostgreSQL remains an optional integration smoke test for the real
`node-red-contrib-postgresql` message contract. With the Compose demo already
running, verify that new readings reach PostgreSQL:

```sh
docker compose up --build -d --wait
npm run test:postgres
```

This smoke test is intentionally separate from both `npm test` and the
simulated outage suite.

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
`Batch outbox` tab claims one payload array with its `msg.outbox.batch` leases,
executes one PostgreSQL `unnest` upsert, and passes the batch directly to
batch-aware settlement.

The host `./demo` directory is mounted at `/data`, so deploying in the Node-RED
editor writes directly to `./demo/flows.json` and changes survive container
restarts. Mounting the containing directory is intentional: Node-RED saves by
writing a temporary file and atomically renaming it over `flows.json`, which
cannot work when `flows.json` itself is an individual Docker bind mount. The
SQLite outbox remains separately persisted in `./data/outbox`.

### No-SQL write-plan and batch demo

[`demo/flows_nosql.json`](demo/flows_nosql.json) is an alternate, editable
flow showing how a producer can describe writes without owning SQL:

```js
msg.payload = {
    version: 1,
    eventId: "stable-source-event-id",
    writes: {
        sensor_readings: [{
            device_id: "sensor-1",
            observed_at: "2026-07-28T12:00:00.000Z",
            value_mm: 12.5
        }],
        sensor_health: [{
            device_id: "sensor-1",
            observed_at: "2026-07-28T12:00:00.000Z",
            batt_mv: 3710,
            rssi_dbm: -67
        }]
    }
};
```

Start that flow instead of the main demo:

```sh
FLOWS=flows_nosql.json docker compose up --build -d --wait
```

The flow durably enqueues each `WritePlan` as one job, then claims jobs in
batches. A single allowlisted registry validates table names, exact columns,
types, required values, conflict keys, explicit conflict-update columns, and
table order. Its closed type catalog maps logical types such as `float` and
`timestamptz` to both validation rules and trusted PostgreSQL types, so
producers cannot supply SQL fragments. Optional columns are deliberately
rejected until their insert-default and conflict-update behavior is explicitly
defined.
Invalid plans are split out with their matching leases and dead-lettered as
`data`; valid plans are collated by table and compiled into one parameterized
PostgreSQL statement. Data-modifying CTEs and `jsonb_to_recordset` make all
table upserts one atomic transaction. A PostgreSQL failure therefore retries
the complete valid lease subset without partially acknowledging it.

The registry and plan version form part of the durable sink protocol. Every
subflow instance that shares a sink key must use the same registry. Change the
version suffix in the sink key when making an incompatible plan or registry
change; otherwise a worker can claim a persisted job it does not understand.

The producer runs at five plans per second while the worker polls once per
second, so each execution normally demonstrates a five-row batch. Click
`enqueue invalid plan` to exercise validation and the dead-letter output.
The operation group includes status, resume, and dead-letter inspection
controls.

#### Durable error logging

The same demo includes `Log Error → Durable WritePlan`. Logger instances
sanitize caught Node-RED errors and enqueue an `errors` WritePlan directly to
the selected sink. They do not contain claim pollers; one shared No-SQL Outbox
instance owns PostgreSQL delivery for the sink. This avoids creating one worker
for every Catch/Log Error instance.

The plan stores a stable `event_id` derived from the source node, Node-RED
message ID, and error occurrence. Both the outbox dedupe key and PostgreSQL
conflict key use that identity. Nested `Error` objects, circular causes, dates,
buffers, and oversized diagnostic structures are converted to bounded JSON
before enqueue.

For an existing `errors` table, apply this once before using the new logger:

```sql
ALTER TABLE errors
    ADD COLUMN IF NOT EXISTS event_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS errors_event_id_time_uidx
    ON errors (event_id, time);
```

Including `time` makes the unique index compatible with a TimescaleDB
hypertable partitioned on that column. Existing rows may leave `event_id`
null; every new error plan supplies it. The demo's fresh-table bootstrap
already creates the equivalent constraint.

Because `./demo` is mounted as a directory, editor deployments save this
alternate flow directly to `./demo/flows_nosql.json`. Return to the normal
demo with:

```sh
docker compose up -d --wait --force-recreate node-red
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
msg.outbox.action = "requeue-dead";
msg.outbox.sink = "postgres-demo";
msg.outbox.failureClass = "infrastructure";
msg.outbox.limit = 1000;
return msg;
```

To replay one reviewed dead letter:

```js
msg.outbox.action = "requeue-one";
msg.outbox.deadLetterId = "the-job-id";
return msg;
```

To purge at most 1,000 delivered records older than one day:

```js
msg.outbox.action = "purge-delivered";
msg.outbox.olderThanMs = 24 * 60 * 60 * 1000;
msg.outbox.limit = 1000;
return msg;
```

After cleanup, checkpoint the WAL. Vacuum is opt-in because it can block
ingestion:

```js
msg.outbox.action = "maintenance";
msg.outbox.vacuum = false;
return msg;
```

There are no built-in HTTP management endpoints. Authentication, authorization,
auditing, and operator presentation remain part of the surrounding Node-RED
flow.

## Operational notes

- Mount the SQLite directory on persistent local storage. Do not place the WAL
  database on NFS.
- Monitor `status` health fields for queue depth, oldest queued age, expired
  leases, dead-letter count, logical database use, reusable pages, filesystem
  free space, capacity state, cleanup totals, pressure evictions, and startup
  integrity. Pressure eviction indicates that the effective delivered-history
  and deduplication window was shortened to preserve ingestion.
- Wire the second `outbox-enqueue` output and alert on
  `OUTBOX_DATABASE_CAPACITY`, `OUTBOX_QUEUE_CAPACITY`, and
  `OUTBOX_DISK_HEADROOM`. A database-capacity rejection means no reclaimable
  delivered history remained; filesystem headroom remains an independent hard
  boundary.
- Run `check-integrity` periodically and investigate `ok: false` before
  resuming ingestion. Startup recreates the queue-count triggers and reconciles
  their counters transactionally.
- A worker lease should exceed the sink's connection and request timeout.
- Automatic bounded delivered-history cleanup is owned by the durable outbox;
  an Inject node is not required. Keep `purge-delivered` for operator-requested
  cleanup. Run an optional maintenance vacuum only during a quiet window.
- The outbox gives PostgreSQL effectively-once behavior because the demo uses
  an idempotent upsert. External HTTP sinks still need an idempotency key to
  avoid duplicates after an ambiguous timeout.
