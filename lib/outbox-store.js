"use strict";

const { createHash, randomUUID } = require("node:crypto");
const { mkdirSync } = require("node:fs");
const { dirname } = require("node:path");
const { DatabaseSync } = require("node:sqlite");

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(",")}}`;
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
      "dead_letter_jobs",
      "retry_until_expired",
      "INTEGER NOT NULL DEFAULT 0"
    );
    this.addColumnIfMissing("dead_letter_jobs", "last_failure_class", "TEXT");
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

  enqueue(jobs) {
    const input = Array.isArray(jobs) ? jobs : [jobs];
    if (!input.length) return [];
    const now = this.now();
    const insert = this.db.prepare(`
      INSERT INTO outbox_jobs (
        id, dedupe_key, sink, schema_version, payload_json,
        max_attempts, retry_until_expired, max_age_ms,
        base_delay_ms, max_delay_ms,
        available_at, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(dedupe_key) DO NOTHING
    `);
    const existing = this.db.prepare(
      "SELECT id, state FROM outbox_jobs WHERE dedupe_key = ?"
    );
    const existingDead = this.db.prepare(
      "SELECT id, 'dead' AS state FROM dead_letter_jobs WHERE dedupe_key = ?"
    );

    return this.transaction(() =>
      input.map((job) => {
        if (!job || typeof job !== "object") {
          throw new TypeError("Every outbox job must be an object");
        }
        if (!job.sink || typeof job.sink !== "string") {
          throw new TypeError("Every outbox job requires a string sink");
        }
        if (!Object.prototype.hasOwnProperty.call(job, "payload")) {
          throw new TypeError("Every outbox job requires a payload");
        }

        const payloadJson = stableStringify(job.payload);
        const dedupeKey =
          job.dedupeKey ||
          createHash("sha256")
            .update(`${job.sink}\n${payloadJson}`)
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
          return { id, dedupeKey, inserted: true, state: "pending" };
        }
        const duplicate = existing.get(dedupeKey) || existingDead.get(dedupeKey);
        return {
          id: duplicate.id,
          dedupeKey,
          inserted: false,
          state: duplicate.state,
        };
      })
    );
  }

  claim({ sink, leaseMs = 60_000, maxInFlight = 1 } = {}) {
    if (!sink) throw new Error("A sink is required to claim an outbox job");
    const now = this.now();

    return this.transaction(() => {
      const control = this.getSinkControl(sink);
      if (
        control.manuallyPaused ||
        (control.pausedUntil != null && control.pausedUntil > now)
      ) {
        return null;
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
          id, dedupe_key, sink, schema_version, payload_json,
          attempts, max_attempts, retry_until_expired, max_age_ms,
          base_delay_ms, max_delay_ms, first_error, last_error,
          last_status, last_failure_class, created_at, failed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
      if (active.count >= maxInFlight) return null;

      const candidate = this.db
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
            LIMIT 1`
        )
        .get(sink, now, now);
      if (!candidate) return null;

      const row = this.db
        .prepare(
          `UPDATE outbox_jobs
              SET state = 'leased',
                  attempts = attempts + 1,
                  lease_until = ?
            WHERE id = ?
            RETURNING *`
        )
        .get(now + leaseMs, candidate.id);
      return this.parseJob(row);
    });
  }

  settle(id, outcome = {}) {
    if (!id) throw new Error("An outbox job id is required");
    const now = this.now();

    return this.transaction(() => {
      const row = this.db
        .prepare("SELECT * FROM outbox_jobs WHERE id = ?")
        .get(id);
      if (!row) throw new Error(`Outbox job ${id} does not exist`);
      if (row.state !== "leased") {
        throw new Error(`Outbox job ${id} is not leased`);
      }

      if (outcome.success) {
        this.db
          .prepare(
            `UPDATE outbox_jobs
                SET state = 'delivered', delivered_at = ?, lease_until = NULL
              WHERE id = ?`
          )
          .run(now, id);
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
              id, dedupe_key, sink, schema_version, payload_json,
              attempts, max_attempts, retry_until_expired, max_age_ms,
              base_delay_ms, max_delay_ms, first_error, last_error,
              last_status, last_failure_class, created_at, failed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .run(
            row.id,
            row.dedupe_key,
            row.sink,
            row.schema_version,
            row.payload_json,
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
                  first_error = COALESCE(first_error, ?),
                  last_error = ?,
                  last_status = ?,
                  last_failure_class = ?
            WHERE id = ?`
        )
        .run(availableAt, errorText, errorText, status, failureClass, id);
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

  listDeadLetters(limit = 100) {
    return this.db
      .prepare(
        "SELECT * FROM dead_letter_jobs ORDER BY failed_at DESC LIMIT ?"
      )
      .all(limit)
      .map((row) => this.parseJob(row));
  }

  requeueDeadLetter(id) {
    const now = this.now();
    return this.transaction(() => {
      const row = this.db
        .prepare("SELECT * FROM dead_letter_jobs WHERE id = ?")
        .get(id);
      if (!row) throw new Error(`Dead-letter job ${id} does not exist`);
      this.db
        .prepare(
          `INSERT INTO outbox_jobs (
            id, dedupe_key, sink, schema_version, payload_json,
            state, attempts, max_attempts, retry_until_expired, max_age_ms,
            base_delay_ms, max_delay_ms, available_at, created_at
          ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          row.id,
          row.dedupe_key,
          row.sink,
          row.schema_version,
          row.payload_json,
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
      const insert = this.db.prepare(
        `INSERT INTO outbox_jobs (
          id, dedupe_key, sink, schema_version, payload_json,
          state, attempts, max_attempts, retry_until_expired, max_age_ms,
          base_delay_ms, max_delay_ms, available_at, created_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
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

  stats() {
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
    return { jobs: rows, deadLetters: dead.count, controls };
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
      payload: JSON.parse(row.payload_json),
      state: row.state || "dead",
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      retryUntilExpired: Boolean(row.retry_until_expired),
      maxAgeMs: row.max_age_ms,
      baseDelayMs: row.base_delay_ms,
      maxDelayMs: row.max_delay_ms,
      availableAt: row.available_at,
      leaseUntil: row.lease_until,
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
};
