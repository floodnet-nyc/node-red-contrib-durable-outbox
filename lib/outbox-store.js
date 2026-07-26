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
        max_age_ms      INTEGER NOT NULL DEFAULT 86400000,
        base_delay_ms   INTEGER NOT NULL DEFAULT 2000,
        max_delay_ms    INTEGER NOT NULL DEFAULT 300000,
        available_at    INTEGER NOT NULL,
        lease_until     INTEGER,
        first_error     TEXT,
        last_error      TEXT,
        last_status     INTEGER,
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
        max_age_ms      INTEGER NOT NULL,
        base_delay_ms   INTEGER NOT NULL,
        max_delay_ms    INTEGER NOT NULL,
        first_error     TEXT,
        last_error      TEXT,
        last_status     INTEGER,
        created_at      INTEGER NOT NULL,
        failed_at       INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS dead_letter_failed_at
        ON dead_letter_jobs (failed_at);
    `);
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
        max_attempts, max_age_ms, base_delay_ms, max_delay_ms,
        available_at, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                attempts >= max_attempts
                OR ? - created_at >= max_age_ms
              )`
        )
        .all(sink, now, now);
      const insertDead = this.db.prepare(
        `INSERT INTO dead_letter_jobs (
          id, dedupe_key, sink, schema_version, payload_json,
          attempts, max_attempts, max_age_ms, base_delay_ms, max_delay_ms,
          first_error, last_error, last_status, created_at, failed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
          row.max_age_ms,
          row.base_delay_ms,
          row.max_delay_ms,
          row.first_error || reason,
          reason,
          row.last_status,
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
        return { id, state: "delivered", attempts: row.attempts };
      }

      const errorText = serializeError(outcome.error);
      const status = outcome.status == null ? null : Number(outcome.status);
      const expired = now - row.created_at >= row.max_age_ms;
      const exhausted = row.attempts >= row.max_attempts;
      const dead = outcome.retryable === false || expired || exhausted;

      if (dead) {
        this.db
          .prepare(
            `INSERT INTO dead_letter_jobs (
              id, dedupe_key, sink, schema_version, payload_json,
              attempts, max_attempts, max_age_ms, base_delay_ms, max_delay_ms,
              first_error, last_error, last_status, created_at, failed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .run(
            row.id,
            row.dedupe_key,
            row.sink,
            row.schema_version,
            row.payload_json,
            row.attempts,
            row.max_attempts,
            row.max_age_ms,
            row.base_delay_ms,
            row.max_delay_ms,
            row.first_error || errorText,
            errorText,
            status,
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
                  last_status = ?
            WHERE id = ?`
        )
        .run(availableAt, errorText, errorText, status, id);
      return {
        id,
        state: "pending",
        attempts: row.attempts,
        delayMs,
        availableAt,
      };
    });
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
            state, attempts, max_attempts, max_age_ms, base_delay_ms,
            max_delay_ms, available_at, created_at
          ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          row.id,
          row.dedupe_key,
          row.sink,
          row.schema_version,
          row.payload_json,
          row.max_attempts,
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
    return { jobs: rows, deadLetters: dead.count };
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
      maxAgeMs: row.max_age_ms,
      baseDelayMs: row.base_delay_ms,
      maxDelayMs: row.max_delay_ms,
      availableAt: row.available_at,
      leaseUntil: row.lease_until,
      firstError: row.first_error,
      lastError: row.last_error,
      lastStatus: row.last_status,
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
