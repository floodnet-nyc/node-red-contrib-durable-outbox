"use strict";

const { createHash, randomUUID } = require("node:crypto");
const { mkdirSync, statSync } = require("node:fs");
const { dirname } = require("node:path");
const { DatabaseSync } = require("node:sqlite");

function isPlainObject(value) {
  if (value === null || typeof value !== "object") return false;
  const prototype = Object.getPrototypeOf(value);
  if (prototype === null || prototype === Object.prototype) return true;
  return (
    Object.prototype.toString.call(value) === "[object Object]" &&
    prototype.constructor?.name === "Object"
  );
}

function canonicalize(value, seen = new Set(), location = "payload") {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (value === undefined) {
    return undefined;
  }
  if (
    typeof value === "bigint" ||
    typeof value === "function" ||
    typeof value === "symbol"
  ) {
    throw new TypeError(`Unsupported ${typeof value} at ${location}`);
  }
  if (seen.has(value)) {
    throw new TypeError(`Circular reference at ${location}`);
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new TypeError(`Invalid Date at ${location}`);
    }
    return { $outboxType: "date", value: value.toISOString() };
  }
  if (Buffer.isBuffer(value)) {
    return { $outboxType: "buffer", value: value.toString("base64") };
  }
  if (!Array.isArray(value) && !isPlainObject(value)) {
    throw new TypeError(
      `Unsupported object type ${value.constructor?.name || "unknown"} at ${location}`
    );
  }

  seen.add(value);
  let result;
  if (Array.isArray(value)) {
    result = value.map((item, index) => {
      const encoded = canonicalize(item, seen, `${location}[${index}]`);
      return encoded === undefined ? null : encoded;
    });
  } else {
    result = {};
    for (const key of Object.keys(value).sort()) {
      const encoded = canonicalize(value[key], seen, `${location}.${key}`);
      if (encoded !== undefined) result[key] = encoded;
    }
  }
  seen.delete(value);
  return result;
}

function stableStringify(value) {
  const canonical = canonicalize(value);
  if (canonical === undefined) {
    throw new TypeError("The outbox payload cannot be undefined");
  }
  if (containsExtendedType(value)) {
    return `canonical-v2:${JSON.stringify(encodeNode(value))}`;
  }
  return JSON.stringify(canonical);
}

function containsExtendedType(value, seen = new Set()) {
  if (value === null || typeof value !== "object") return false;
  if (value instanceof Date || Buffer.isBuffer(value)) return true;
  if (seen.has(value)) return false;
  seen.add(value);
  const result = Array.isArray(value)
    ? value.some((item) => containsExtendedType(item, seen))
    : Object.keys(value).some((key) => containsExtendedType(value[key], seen));
  seen.delete(value);
  return result;
}

function encodeNode(value, seen = new Set(), location = "payload") {
  if (value === null) return ["null"];
  if (typeof value === "string") return ["string", value];
  if (typeof value === "boolean") return ["boolean", value];
  if (typeof value === "number") {
    return ["number", Number.isFinite(value) ? value : null];
  }
  if (value === undefined) return ["undefined"];
  if (
    typeof value === "bigint" ||
    typeof value === "function" ||
    typeof value === "symbol"
  ) {
    throw new TypeError(`Unsupported ${typeof value} at ${location}`);
  }
  if (seen.has(value)) throw new TypeError(`Circular reference at ${location}`);
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new TypeError(`Invalid Date at ${location}`);
    return ["date", value.toISOString()];
  }
  if (Buffer.isBuffer(value)) return ["buffer", value.toString("base64")];
  if (!Array.isArray(value) && !isPlainObject(value)) {
    throw new TypeError(
      `Unsupported object type ${value.constructor?.name || "unknown"} at ${location}`
    );
  }

  seen.add(value);
  let encoded;
  if (Array.isArray(value)) {
    encoded = [
      "array",
      value.map((item, index) => {
        if (item === undefined) return ["null"];
        return encodeNode(item, seen, `${location}[${index}]`);
      }),
    ];
  } else {
    encoded = [
      "object",
      Object.keys(value)
        .sort()
        .filter((key) => value[key] !== undefined)
        .map((key) => [key, encodeNode(value[key], seen, `${location}.${key}`)]),
    ];
  }
  seen.delete(value);
  return encoded;
}

function decodeNode(node) {
  const [type, value] = node;
  switch (type) {
    case "null":
      return null;
    case "string":
    case "boolean":
    case "number":
      return value;
    case "date":
      return new Date(value);
    case "buffer":
      return Buffer.from(value, "base64");
    case "array":
      return value.map(decodeNode);
    case "object":
      return Object.fromEntries(value.map(([key, item]) => [key, decodeNode(item)]));
    default:
      throw new Error(`Unknown outbox payload type: ${type}`);
  }
}

function encodePayload(value) {
  if (value === undefined) {
    throw new TypeError("The outbox payload cannot be undefined");
  }
  return JSON.stringify({
    format: "node-red-outbox",
    version: 2,
    value: encodeNode(value),
  });
}

function decodePayload(json, encoding = "json-v1") {
  const parsed = JSON.parse(json);
  if (encoding === "canonical-v2") {
    if (parsed?.format !== "node-red-outbox" || parsed.version !== 2) {
      throw new Error("Invalid canonical-v2 outbox payload");
    }
    return decodeNode(parsed.value);
  }
  return parsed;
}

function serializeError(error) {
  if (error == null) return null;
  if (typeof error === "string") return error;
  if (error instanceof Error) {
    return JSON.stringify({
      name: error.name,
      message: error.message,
      code: error.code,
      stack: error.stack,
    });
  }
  return stableStringify(error);
}

class OutboxStore {
  constructor(filename, options = {}) {
    if (!filename) throw new Error("An outbox SQLite filename is required");
    if (filename !== ":memory:") mkdirSync(dirname(filename), { recursive: true });

    this.filename = filename;
    this.now = options.now || Date.now;
    this.random = options.random || Math.random;
    this.maxQueuedJobs = Math.max(1, Number(options.maxQueuedJobs) || 100_000);
    this.maxJobBytes = Math.max(1, Number(options.maxJobBytes) || 1_048_576);
    this.maxEnqueueBatch = Math.max(
      1,
      Number(options.maxEnqueueBatch) || 1_000
    );
    this.maxDatabaseBytes = Math.max(
      1,
      Number(options.maxDatabaseBytes) || 1_073_741_824
    );
    this.db = new DatabaseSync(filename);
    this.db.exec("PRAGMA foreign_keys = ON");
    this.db.exec("PRAGMA busy_timeout = 5000");
    if (filename !== ":memory:") {
      this.db.exec("PRAGMA journal_mode = WAL");
      this.db.exec("PRAGMA synchronous = FULL");
    }
    this.migrate();
  }

  migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS outbox_jobs (
        id              TEXT PRIMARY KEY,
        dedupe_key      TEXT NOT NULL UNIQUE,
        sink            TEXT NOT NULL,
        schema_version  INTEGER NOT NULL DEFAULT 1,
        payload_json    TEXT NOT NULL,
        payload_encoding TEXT NOT NULL DEFAULT 'json-v1',
        state           TEXT NOT NULL DEFAULT 'pending'
                        CHECK (state IN ('pending', 'leased', 'delivered')),
        attempts        INTEGER NOT NULL DEFAULT 0,
        max_attempts    INTEGER NOT NULL DEFAULT 10,
        retry_until_expired INTEGER NOT NULL DEFAULT 0,
        max_age_ms      INTEGER NOT NULL DEFAULT 86400000,
        base_delay_ms   INTEGER NOT NULL DEFAULT 2000,
        max_delay_ms    INTEGER NOT NULL DEFAULT 300000,
        available_at    INTEGER NOT NULL,
        lease_until     INTEGER,
        lease_token     TEXT,
        first_error     TEXT,
        last_error      TEXT,
        last_status     INTEGER,
        last_failure_class TEXT,
        created_at      INTEGER NOT NULL,
        delivered_at    INTEGER
      );

      CREATE INDEX IF NOT EXISTS outbox_ready
        ON outbox_jobs (sink, state, available_at, created_at);

      CREATE TABLE IF NOT EXISTS dead_letter_jobs (
        id              TEXT PRIMARY KEY,
        dedupe_key      TEXT NOT NULL,
        sink            TEXT NOT NULL,
        schema_version  INTEGER NOT NULL,
        payload_json    TEXT NOT NULL,
        payload_encoding TEXT NOT NULL DEFAULT 'json-v1',
        attempts        INTEGER NOT NULL,
        max_attempts    INTEGER NOT NULL,
        retry_until_expired INTEGER NOT NULL DEFAULT 0,
        max_age_ms      INTEGER NOT NULL,
        base_delay_ms   INTEGER NOT NULL,
        max_delay_ms    INTEGER NOT NULL,
        first_error     TEXT,
        last_error      TEXT,
        last_status     INTEGER,
        last_failure_class TEXT,
        created_at      INTEGER NOT NULL,
        failed_at       INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS dead_letter_failed_at
        ON dead_letter_jobs (failed_at);

      CREATE TABLE IF NOT EXISTS outbox_sink_controls (
        sink                  TEXT PRIMARY KEY,
        manually_paused       INTEGER NOT NULL DEFAULT 0,
        paused_until          INTEGER,
        consecutive_failures  INTEGER NOT NULL DEFAULT 0,
        last_failure_at       INTEGER,
        last_success_at       INTEGER,
        last_error            TEXT
      );
    `);

    this.addColumnIfMissing(
      "outbox_jobs",
      "retry_until_expired",
      "INTEGER NOT NULL DEFAULT 0"
    );
    this.addColumnIfMissing("outbox_jobs", "last_failure_class", "TEXT");
    this.addColumnIfMissing(
      "outbox_jobs",
      "payload_encoding",
      "TEXT NOT NULL DEFAULT 'json-v1'"
    );
    this.addColumnIfMissing("outbox_jobs", "lease_token", "TEXT");
    this.addColumnIfMissing(
      "dead_letter_jobs",
      "retry_until_expired",
      "INTEGER NOT NULL DEFAULT 0"
    );
    this.addColumnIfMissing("dead_letter_jobs", "last_failure_class", "TEXT");
    this.addColumnIfMissing(
      "dead_letter_jobs",
      "payload_encoding",
      "TEXT NOT NULL DEFAULT 'json-v1'"
    );
  }

  addColumnIfMissing(table, column, definition) {
    const columns = this.db.prepare(`PRAGMA table_info("${table}")`).all();
    if (!columns.some((item) => item.name === column)) {
      this.db.exec(
        `ALTER TABLE "${table}" ADD COLUMN "${column}" ${definition}`
      );
    }
  }

  transaction(fn) {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  assertQueueCapacity(additional) {
    const queued = this.db
      .prepare(
        "SELECT COUNT(*) AS count FROM outbox_jobs WHERE state != 'delivered'"
      )
      .get().count;
    if (queued + additional > this.maxQueuedJobs) {
      throw new RangeError(
        `Outbox queue exceeds the ${this.maxQueuedJobs} job limit`
      );
    }
  }

  enqueue(jobs) {
    const input = Array.isArray(jobs) ? jobs : [jobs];
    if (!input.length) return [];
    if (input.length > this.maxEnqueueBatch) {
      throw new RangeError(
        `Outbox enqueue batch exceeds the ${this.maxEnqueueBatch} job limit`
      );
    }
    const now = this.now();
    const insert = this.db.prepare(`
      INSERT INTO outbox_jobs (
        id, dedupe_key, sink, schema_version, payload_json, payload_encoding,
        max_attempts, retry_until_expired, max_age_ms,
        base_delay_ms, max_delay_ms,
        available_at, created_at
      ) VALUES (?, ?, ?, ?, ?, 'canonical-v2', ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(dedupe_key) DO NOTHING
    `);
    const existing = this.db.prepare(
      "SELECT id, state FROM outbox_jobs WHERE dedupe_key = ?"
    );
    const existingDead = this.db.prepare(
      "SELECT id, 'dead' AS state FROM dead_letter_jobs WHERE dedupe_key = ?"
    );

    return this.transaction(() => {
      let queued = this.db
        .prepare(
          "SELECT COUNT(*) AS count FROM outbox_jobs WHERE state != 'delivered'"
        )
        .get().count;
      return input.map((job) => {
        if (!job || typeof job !== "object") {
          throw new TypeError("Every outbox job must be an object");
        }
        if (!job.sink || typeof job.sink !== "string") {
          throw new TypeError("Every outbox job requires a string sink");
        }
        if (!Object.prototype.hasOwnProperty.call(job, "payload")) {
          throw new TypeError("Every outbox job requires a payload");
        }

        const canonicalPayload = stableStringify(job.payload);
        const payloadJson = encodePayload(job.payload);
        if (Buffer.byteLength(payloadJson, "utf8") > this.maxJobBytes) {
          throw new RangeError(
            `Outbox payload exceeds the ${this.maxJobBytes} byte limit`
          );
        }
        const dedupeKey =
          job.dedupeKey ||
          createHash("sha256")
            .update(`${job.sink}\n${canonicalPayload}`)
            .digest("hex");
        const id = job.id || randomUUID();
        const result = insert.run(
          id,
          dedupeKey,
          job.sink,
          job.schemaVersion ?? 1,
          payloadJson,
          job.maxAttempts ?? 10,
          job.retryUntilExpired ? 1 : 0,
          job.maxAgeMs ?? 86_400_000,
          job.baseDelayMs ?? 2_000,
          job.maxDelayMs ?? 300_000,
          job.availableAt ?? now,
          now
        );
        if (result.changes === 1) {
          queued += 1;
          if (queued > this.maxQueuedJobs) {
            throw new RangeError(
              `Outbox queue exceeds the ${this.maxQueuedJobs} job limit`
            );
          }
          return { id, dedupeKey, inserted: true, state: "pending" };
        }
        const duplicate = existing.get(dedupeKey) || existingDead.get(dedupeKey);
        return {
          id: duplicate.id,
          dedupeKey,
          inserted: false,
          state: duplicate.state,
        };
      });
    });
  }

  claim(options = {}) {
    return this.claimBatch({ ...options, batchSize: 1 })[0] || null;
  }

  claimBatch({
    sink,
    leaseMs = 60_000,
    maxInFlight = 1,
    batchSize = 10,
  } = {}) {
    if (!sink) throw new Error("A sink is required to claim an outbox job");
    const now = this.now();
    const safeMaxInFlight = Math.max(1, Number(maxInFlight) || 1);
    const safeBatchSize = Math.max(
      1,
      Math.min(1_000, Number(batchSize) || 10)
    );
    const safeLeaseMs = Math.max(1, Number(leaseMs) || 60_000);

    return this.transaction(() => {
      const control = this.getSinkControl(sink);
      if (
        control.manuallyPaused ||
        (control.pausedUntil != null && control.pausedUntil > now)
      ) {
        return [];
      }

      const exhausted = this.db
        .prepare(
          `SELECT *
             FROM outbox_jobs
            WHERE sink = ?
              AND state != 'delivered'
              AND (
                state = 'pending'
                OR (state = 'leased' AND lease_until <= ?)
              )
              AND (
                (retry_until_expired = 0 AND attempts >= max_attempts)
                OR ? - created_at >= max_age_ms
              )`
        )
        .all(sink, now, now);
      const insertDead = this.db.prepare(
        `INSERT INTO dead_letter_jobs (
          id, dedupe_key, sink, schema_version, payload_json, payload_encoding,
          attempts, max_attempts, retry_until_expired, max_age_ms,
          base_delay_ms, max_delay_ms, first_error, last_error,
          last_status, last_failure_class, created_at, failed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      const deleteJob = this.db.prepare(
        "DELETE FROM outbox_jobs WHERE id = ?"
      );
      for (const row of exhausted) {
        const reason =
          row.attempts >= row.max_attempts
            ? "Lease expired after maximum delivery attempts"
            : "Job exceeded maximum delivery age";
        insertDead.run(
          row.id,
          row.dedupe_key,
          row.sink,
          row.schema_version,
          row.payload_json,
          row.payload_encoding,
          row.attempts,
          row.max_attempts,
          row.retry_until_expired,
          row.max_age_ms,
          row.base_delay_ms,
          row.max_delay_ms,
          row.first_error || reason,
          reason,
          row.last_status,
          row.last_failure_class || "infrastructure",
          row.created_at,
          now
        );
        deleteJob.run(row.id);
      }

      const active = this.db
        .prepare(
          `SELECT COUNT(*) AS count
             FROM outbox_jobs
            WHERE sink = ? AND state = 'leased' AND lease_until > ?`
        )
        .get(sink, now);
      const capacity = Math.min(
        safeBatchSize,
        safeMaxInFlight - active.count
      );
      if (capacity <= 0) return [];

      const candidates = this.db
        .prepare(
          `SELECT id
             FROM outbox_jobs
            WHERE sink = ?
              AND available_at <= ?
              AND (
                state = 'pending'
                OR (state = 'leased' AND lease_until <= ?)
              )
            ORDER BY created_at, id
            LIMIT ?`
        )
        .all(sink, now, now, capacity);
      if (!candidates.length) return [];

      const lease = this.db.prepare(
          `UPDATE outbox_jobs
              SET state = 'leased',
                  attempts = attempts + 1,
                  lease_until = ?,
                  lease_token = ?
            WHERE id = ?
            RETURNING *`
        );
      return candidates.map((candidate) => {
        const token = randomUUID();
        const row = lease.get(now + safeLeaseMs, token, candidate.id);
        return this.parseJob(row);
      });
    });
  }

  settle(id, outcome = {}) {
    if (!id) throw new Error("An outbox job id is required");
    if (!outcome.leaseToken || typeof outcome.leaseToken !== "string") {
      throw new Error("A lease token is required to settle an outbox job");
    }
    const now = this.now();

    return this.transaction(() => {
      const row = this.db
        .prepare("SELECT * FROM outbox_jobs WHERE id = ?")
        .get(id);
      if (!row) throw new Error(`Outbox job ${id} does not exist`);
      if (row.state !== "leased") {
        return { id, state: "stale_lease" };
      }
      if (row.lease_token !== outcome.leaseToken) {
        return { id, state: "stale_lease" };
      }

      if (outcome.success) {
        this.db
          .prepare(
            `UPDATE outbox_jobs
                SET state = 'delivered', delivered_at = ?,
                    lease_until = NULL, lease_token = NULL
              WHERE id = ? AND lease_token = ?`
          )
          .run(now, id, outcome.leaseToken);
        this.recordSinkSuccess(row.sink, now);
        return { id, state: "delivered", attempts: row.attempts };
      }

      const errorText = serializeError(outcome.error);
      const status = outcome.status == null ? null : Number(outcome.status);
      const failureClass = outcome.failureClass || "unknown";
      const expired = now - row.created_at >= row.max_age_ms;
      const exhausted = row.attempts >= row.max_attempts;
      const dead =
        outcome.retryable === false ||
        expired ||
        (!row.retry_until_expired && exhausted);

      if (dead) {
        this.db
          .prepare(
            `INSERT INTO dead_letter_jobs (
              id, dedupe_key, sink, schema_version, payload_json, payload_encoding,
              attempts, max_attempts, retry_until_expired, max_age_ms,
              base_delay_ms, max_delay_ms, first_error, last_error,
              last_status, last_failure_class, created_at, failed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .run(
            row.id,
            row.dedupe_key,
            row.sink,
            row.schema_version,
            row.payload_json,
            row.payload_encoding,
            row.attempts,
            row.max_attempts,
            row.retry_until_expired,
            row.max_age_ms,
            row.base_delay_ms,
            row.max_delay_ms,
            row.first_error || errorText,
            errorText,
            status,
            failureClass,
            row.created_at,
            now
          );
        this.db.prepare("DELETE FROM outbox_jobs WHERE id = ?").run(id);
        return {
          id,
          state: "dead",
          attempts: row.attempts,
          reason: outcome.retryable === false
            ? "non-retryable"
            : expired
              ? "expired"
              : "attempts-exhausted",
        };
      }

      const ceiling = Math.min(
        row.max_delay_ms,
        row.base_delay_ms * 2 ** Math.max(0, row.attempts - 1)
      );
      const delayMs = Math.floor(this.random() * ceiling);
      const availableAt = now + delayMs;
      this.db
        .prepare(
          `UPDATE outbox_jobs
              SET state = 'pending',
                  available_at = ?,
                  lease_until = NULL,
                  lease_token = NULL,
                  first_error = COALESCE(first_error, ?),
                  last_error = ?,
                  last_status = ?,
                  last_failure_class = ?
            WHERE id = ? AND lease_token = ?`
        )
        .run(
          availableAt,
          errorText,
          errorText,
          status,
          failureClass,
          id,
          outcome.leaseToken
        );
      const circuit = this.recordSinkFailure(row.sink, {
        now,
        errorText,
        failureClass,
        threshold: outcome.circuitBreakerThreshold,
        cooldownMs: outcome.circuitBreakerCooldownMs,
      });
      return {
        id,
        state: "pending",
        attempts: row.attempts,
        delayMs,
        availableAt,
        circuit,
      };
    });
  }

  getSinkControl(sink) {
    const row = this.db
      .prepare(
        `SELECT sink, manually_paused, paused_until, consecutive_failures,
                last_failure_at, last_success_at, last_error
           FROM outbox_sink_controls
          WHERE sink = ?`
      )
      .get(sink);
    return row
      ? {
          sink: row.sink,
          manuallyPaused: Boolean(row.manually_paused),
          pausedUntil: row.paused_until,
          consecutiveFailures: row.consecutive_failures,
          lastFailureAt: row.last_failure_at,
          lastSuccessAt: row.last_success_at,
          lastError: row.last_error,
        }
      : {
          sink,
          manuallyPaused: false,
          pausedUntil: null,
          consecutiveFailures: 0,
          lastFailureAt: null,
          lastSuccessAt: null,
          lastError: null,
        };
  }

  recordSinkFailure(
    sink,
    {
      now = this.now(),
      errorText,
      failureClass,
      threshold = 3,
      cooldownMs = 30_000,
    } = {}
  ) {
    if (failureClass !== "infrastructure") {
      return this.getSinkControl(sink);
    }
    const limit = Math.max(1, Number(threshold) || 3);
    const cooldown = Math.max(1_000, Number(cooldownMs) || 30_000);
    this.db
      .prepare(
        `INSERT INTO outbox_sink_controls (
          sink, consecutive_failures, last_failure_at, last_error
        ) VALUES (?, 1, ?, ?)
        ON CONFLICT(sink) DO UPDATE SET
          consecutive_failures = consecutive_failures + 1,
          last_failure_at = excluded.last_failure_at,
          last_error = excluded.last_error`
      )
      .run(sink, now, errorText);
    const control = this.getSinkControl(sink);
    if (control.consecutiveFailures >= limit) {
      this.db
        .prepare(
          `UPDATE outbox_sink_controls
              SET paused_until = ?
            WHERE sink = ?`
        )
        .run(now + cooldown, sink);
    }
    return this.getSinkControl(sink);
  }

  recordSinkSuccess(sink, now = this.now()) {
    this.db
      .prepare(
        `INSERT INTO outbox_sink_controls (
          sink, consecutive_failures, last_success_at
        ) VALUES (?, 0, ?)
        ON CONFLICT(sink) DO UPDATE SET
          consecutive_failures = 0,
          paused_until = NULL,
          last_success_at = excluded.last_success_at,
          last_error = NULL`
      )
      .run(sink, now);
    return this.getSinkControl(sink);
  }

  pauseSink(sink) {
    this.db
      .prepare(
        `INSERT INTO outbox_sink_controls (sink, manually_paused)
         VALUES (?, 1)
         ON CONFLICT(sink) DO UPDATE SET manually_paused = 1`
      )
      .run(sink);
    return this.getSinkControl(sink);
  }

  resumeSink(sink, { retryNow = true } = {}) {
    const now = this.now();
    return this.transaction(() => {
      this.db
        .prepare(
          `INSERT INTO outbox_sink_controls (
            sink, manually_paused, paused_until, consecutive_failures
          ) VALUES (?, 0, NULL, 0)
          ON CONFLICT(sink) DO UPDATE SET
            manually_paused = 0,
            paused_until = NULL,
            consecutive_failures = 0,
            last_error = NULL`
        )
        .run(sink);
      let released = 0;
      if (retryNow) {
        released = this.db
          .prepare(
            `UPDATE outbox_jobs
                SET available_at = ?
              WHERE sink = ? AND state = 'pending'`
          )
          .run(now, sink).changes;
      }
      return {
        control: this.getSinkControl(sink),
        released,
      };
    });
  }

  retryNow(sink) {
    const now = this.now();
    const result = this.db
      .prepare(
        `UPDATE outbox_jobs
            SET available_at = ?
          WHERE sink = ? AND state = 'pending'`
      )
      .run(now, sink);
    return { sink, released: result.changes, availableAt: now };
  }

  listDeadLetters(options = {}) {
    if (typeof options === "number") options = { limit: options };
    const { sink, failureClass, limit = 100 } = options;
    const clauses = [];
    const params = [];
    if (sink) {
      clauses.push("sink = ?");
      params.push(sink);
    }
    if (failureClass) {
      clauses.push("last_failure_class = ?");
      params.push(failureClass);
    }
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const safeLimit = Math.max(1, Math.min(10_000, Number(limit) || 100));
    return this.db
      .prepare(
        `SELECT * FROM dead_letter_jobs
         ${where}
         ORDER BY failed_at DESC
         LIMIT ?`
      )
      .all(...params, safeLimit)
      .map((row) => this.parseJob(row));
  }

  requeueDeadLetter(id) {
    const now = this.now();
    return this.transaction(() => {
      const row = this.db
        .prepare("SELECT * FROM dead_letter_jobs WHERE id = ?")
        .get(id);
      if (!row) throw new Error(`Dead-letter job ${id} does not exist`);
      this.assertQueueCapacity(1);
      this.db
        .prepare(
          `INSERT INTO outbox_jobs (
            id, dedupe_key, sink, schema_version, payload_json, payload_encoding,
            state, attempts, max_attempts, retry_until_expired, max_age_ms,
            base_delay_ms, max_delay_ms, available_at, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          row.id,
          row.dedupe_key,
          row.sink,
          row.schema_version,
          row.payload_json,
          row.payload_encoding,
          row.max_attempts,
          row.retry_until_expired,
          row.max_age_ms,
          row.base_delay_ms,
          row.max_delay_ms,
          now,
          now
        );
      this.db.prepare("DELETE FROM dead_letter_jobs WHERE id = ?").run(id);
      return { id, state: "pending", availableAt: now };
    });
  }

  requeueDeadLetters({ sink, failureClass, limit = 1_000 } = {}) {
    const now = this.now();
    const clauses = [];
    const params = [];
    if (sink) {
      clauses.push("sink = ?");
      params.push(sink);
    }
    if (failureClass) {
      clauses.push("last_failure_class = ?");
      params.push(failureClass);
    }
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const safeLimit = Math.max(1, Math.min(10_000, Number(limit) || 1_000));

    return this.transaction(() => {
      const rows = this.db
        .prepare(
          `SELECT * FROM dead_letter_jobs
           ${where}
           ORDER BY failed_at
           LIMIT ?`
        )
        .all(...params, safeLimit);
      this.assertQueueCapacity(rows.length);
      const insert = this.db.prepare(
        `INSERT INTO outbox_jobs (
          id, dedupe_key, sink, schema_version, payload_json, payload_encoding,
          state, attempts, max_attempts, retry_until_expired, max_age_ms,
          base_delay_ms, max_delay_ms, available_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
      );
      const remove = this.db.prepare(
        "DELETE FROM dead_letter_jobs WHERE id = ?"
      );
      for (const row of rows) {
        insert.run(
          row.id,
          row.dedupe_key,
          row.sink,
          row.schema_version,
          row.payload_json,
          row.payload_encoding,
          row.max_attempts,
          row.retry_until_expired,
          row.max_age_ms,
          row.base_delay_ms,
          row.max_delay_ms,
          now,
          now
        );
        remove.run(row.id);
      }
      return {
        requeued: rows.length,
        ids: rows.map((row) => row.id),
        sink: sink || null,
        failureClass: failureClass || null,
      };
    });
  }

  deleteDeadLetters({ sink, failureClass, limit = 1_000 } = {}) {
    const clauses = [];
    const params = [];
    if (sink) {
      clauses.push("sink = ?");
      params.push(sink);
    }
    if (failureClass) {
      clauses.push("last_failure_class = ?");
      params.push(failureClass);
    }
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const safeLimit = Math.max(1, Math.min(10_000, Number(limit) || 1_000));
    return this.transaction(() => {
      const rows = this.db
        .prepare(
          `SELECT id FROM dead_letter_jobs
           ${where}
           ORDER BY failed_at
           LIMIT ?`
        )
        .all(...params, safeLimit);
      const remove = this.db.prepare(
        "DELETE FROM dead_letter_jobs WHERE id = ?"
      );
      for (const row of rows) remove.run(row.id);
      return {
        deleted: rows.length,
        ids: rows.map((row) => row.id),
        sink: sink || null,
        failureClass: failureClass || null,
      };
    });
  }

  purgeDelivered({ olderThanMs = 86_400_000, limit = 1_000 } = {}) {
    const age = Math.max(0, Number(olderThanMs) || 0);
    const safeLimit = Math.max(1, Math.min(10_000, Number(limit) || 1_000));
    const cutoff = this.now() - age;
    return this.transaction(() => {
      const rows = this.db
        .prepare(
          `SELECT id FROM outbox_jobs
            WHERE state = 'delivered' AND delivered_at <= ?
            ORDER BY delivered_at
            LIMIT ?`
        )
        .all(cutoff, safeLimit);
      const remove = this.db.prepare("DELETE FROM outbox_jobs WHERE id = ?");
      for (const row of rows) remove.run(row.id);
      return {
        purged: rows.length,
        cutoff,
        olderThanMs: age,
      };
    });
  }

  maintenance({ checkpoint = true, vacuum = false } = {}) {
    const result = { checkpoint: null, vacuumed: false };
    if (checkpoint && this.filename !== ":memory:") {
      result.checkpoint = this.db
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .get();
    }
    if (vacuum) {
      this.db.exec("VACUUM");
      result.vacuumed = true;
    }
    return result;
  }

  fileSize(filename) {
    if (filename === ":memory:") return 0;
    try {
      return statSync(filename).size;
    } catch {
      return 0;
    }
  }

  stats() {
    const now = this.now();
    const rows = this.db
      .prepare(
        `SELECT sink, state, COUNT(*) AS count, MIN(created_at) AS oldest
           FROM outbox_jobs
          GROUP BY sink, state`
      )
      .all();
    const dead = this.db
      .prepare("SELECT COUNT(*) AS count FROM dead_letter_jobs")
      .get();
    const controls = this.db
      .prepare("SELECT * FROM outbox_sink_controls ORDER BY sink")
      .all()
      .map((row) => this.getSinkControl(row.sink));
    const pending = this.db
      .prepare(
        `SELECT
           SUM(CASE WHEN state != 'delivered' THEN 1 ELSE 0 END) AS queued,
           MIN(CASE WHEN state != 'delivered' THEN created_at END) AS oldest,
           SUM(CASE WHEN state = 'leased' AND lease_until <= ? THEN 1 ELSE 0 END)
             AS expired_leases
         FROM outbox_jobs`
      )
      .get(now);
    const databaseBytes = this.fileSize(this.filename);
    const walBytes = this.fileSize(`${this.filename}-wal`);
    return {
      jobs: rows,
      deadLetters: dead.count,
      controls,
      health: {
        queued: pending.queued || 0,
        oldestQueuedAt: pending.oldest,
        oldestQueuedAgeMs:
          pending.oldest == null ? null : Math.max(0, now - pending.oldest),
        expiredLeases: pending.expired_leases || 0,
        databaseBytes,
        walBytes,
        maxDatabaseBytes: this.maxDatabaseBytes,
        databaseSizeWarning:
          databaseBytes + walBytes >= this.maxDatabaseBytes,
      },
    };
  }

  getJob(id) {
    const row = this.db
      .prepare("SELECT * FROM outbox_jobs WHERE id = ?")
      .get(id);
    return row ? this.parseJob(row) : null;
  }

  parseJob(row) {
    return {
      id: row.id,
      dedupeKey: row.dedupe_key,
      sink: row.sink,
      schemaVersion: row.schema_version,
      payload: decodePayload(row.payload_json, row.payload_encoding),
      payloadEncoding: row.payload_encoding || "json-v1",
      state: row.state || "dead",
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      retryUntilExpired: Boolean(row.retry_until_expired),
      maxAgeMs: row.max_age_ms,
      baseDelayMs: row.base_delay_ms,
      maxDelayMs: row.max_delay_ms,
      availableAt: row.available_at,
      leaseUntil: row.lease_until,
      leaseToken: row.lease_token,
      firstError: row.first_error,
      lastError: row.last_error,
      lastStatus: row.last_status,
      lastFailureClass: row.last_failure_class,
      createdAt: row.created_at,
      deliveredAt: row.delivered_at,
      failedAt: row.failed_at,
    };
  }

  close() {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}

module.exports = {
  OutboxStore,
  serializeError,
  stableStringify,
  encodePayload,
  decodePayload,
};
