"use strict";

const { createHash, randomUUID } = require("node:crypto");
const { mkdirSync, statfsSync, statSync } = require("node:fs");
const { dirname } = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const {
  serializeError,
  stableStringify,
  encodePayload,
  serializePayload,
  decodePayload,
} = require("./serialization");
const D = require("./defaults");

const JOB_INSERT_COLUMNS = `
  id, dedupe_key, sink, payload_json, payload_encoding,
  state, attempts, max_attempts, retry_until_expired, max_age_ms,
  base_delay_ms, max_delay_ms, available_at, created_at
`;

const DEAD_INSERT_COLUMNS = `
  id, dedupe_key, sink, payload_json, payload_encoding,
  attempts, max_attempts, retry_until_expired, max_age_ms,
  base_delay_ms, max_delay_ms, first_error, last_error,
  last_status, last_failure_class, created_at, failed_at
`;
const EXHAUSTION_SWEEP_LIMIT = 1_000;
const EXHAUSTION_SWEEP_INTERVAL_MS = 30_000;

function capacityError(code, message, details = {}) {
  const error = new RangeError(message);
  error.code = code;
  Object.assign(error, details);
  return error;
}

class OutboxStore {
  constructor(filename, options = {}) {
    if (!filename) throw new Error("An outbox SQLite filename is required");
    if (filename !== ":memory:") mkdirSync(dirname(filename), { recursive: true });

    this.filename = filename;
    this.now = options.now || Date.now;
    this.random = options.random || Math.random;
    this.nextExhaustionSweep = new Map();
    this.maxQueuedJobs = Math.max(1, Number(options.maxQueuedJobs) || D.MAX_QUEUED_JOBS);
    this.maxJobBytes = Math.max(1, Number(options.maxJobBytes) || D.MAX_JOB_BYTES);
    this.maxEnqueueBatch = Math.max(
      1,
      Number(options.maxEnqueueBatch) || D.MAX_ENQUEUE_BATCH
    );
    this.maxDatabaseBytes = Math.max(
      1,
      Number(options.maxDatabaseBytes) || D.MAX_DATABASE_BYTES
    );
    this.minFreeDiskBytes = Math.max(
      0,
      options.minFreeDiskBytes == null
        ? D.MIN_FREE_DISK_BYTES
        : Number(options.minFreeDiskBytes)
    );
    this.storageCheckIntervalMs = Math.max(
      0,
      options.storageCheckIntervalMs == null
        ? D.STORAGE_CHECK_INTERVAL_MS
        : Number(options.storageCheckIntervalMs)
    );
    this.diskFreeBytesProvider = options.diskFreeBytes ?? null;
    this.cachedDiskSpace = null;
    this.db = new DatabaseSync(filename);
    this.db.exec("PRAGMA foreign_keys = ON");
    this.db.exec("PRAGMA busy_timeout = 5000");
    if (filename !== ":memory:") {
      this.db.exec("PRAGMA journal_mode = WAL");
      this.db.exec("PRAGMA synchronous = FULL");
    }
    this.migrate();
    this.startupIntegrity = this.checkIntegrity({ sqlite: false });
    if (!this.startupIntegrity.ok) {
      const error = new Error(
        `Outbox startup verification failed: ${this.startupIntegrity.errors.join("; ")}`
      );
      this.db.close();
      this.db = null;
      throw error;
    }
  }

  migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS outbox_jobs (
        id              TEXT PRIMARY KEY,
        dedupe_key      TEXT NOT NULL UNIQUE,
        sink            TEXT NOT NULL,
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

      -- The planner serves the pending branch of the claim query from
      -- outbox_ready and the leased branch from outbox_leased_expiry, so a
      -- separate pending-only index is pure write amplification.
      DROP INDEX IF EXISTS outbox_pending_ready;

      CREATE INDEX IF NOT EXISTS outbox_leased_expiry
        ON outbox_jobs (sink, lease_until, created_at, id)
        WHERE state = 'leased';

      CREATE TABLE IF NOT EXISTS dead_letter_jobs (
        id              TEXT PRIMARY KEY,
        dedupe_key      TEXT NOT NULL,
        sink            TEXT NOT NULL,
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
    this.migrateQueueCounters();
  }

  migrateQueueCounters() {
    this.transaction(() => {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS outbox_queue_counts (
          sink    TEXT PRIMARY KEY,
          queued  INTEGER NOT NULL DEFAULT 0 CHECK (queued >= 0)
        );

        DROP TRIGGER IF EXISTS outbox_queue_count_insert;
        DROP TRIGGER IF EXISTS outbox_queue_count_deliver;
        DROP TRIGGER IF EXISTS outbox_queue_count_reactivate;
        DROP TRIGGER IF EXISTS outbox_queue_count_delete;

        CREATE TRIGGER outbox_queue_count_insert
        AFTER INSERT ON outbox_jobs
        WHEN NEW.state != 'delivered'
        BEGIN
          INSERT INTO outbox_queue_counts (sink, queued)
          VALUES (NEW.sink, 1)
          ON CONFLICT(sink) DO UPDATE SET queued = queued + 1;
        END;

        CREATE TRIGGER outbox_queue_count_deliver
        AFTER UPDATE OF state ON outbox_jobs
        WHEN OLD.state != 'delivered' AND NEW.state = 'delivered'
        BEGIN
          UPDATE outbox_queue_counts
             SET queued = MAX(0, queued - 1)
           WHERE sink = OLD.sink;
        END;

        CREATE TRIGGER outbox_queue_count_reactivate
        AFTER UPDATE OF state ON outbox_jobs
        WHEN OLD.state = 'delivered' AND NEW.state != 'delivered'
        BEGIN
          INSERT INTO outbox_queue_counts (sink, queued)
          VALUES (NEW.sink, 1)
          ON CONFLICT(sink) DO UPDATE SET queued = queued + 1;
        END;

        CREATE TRIGGER outbox_queue_count_delete
        AFTER DELETE ON outbox_jobs
        WHEN OLD.state != 'delivered'
        BEGIN
          UPDATE outbox_queue_counts
             SET queued = MAX(0, queued - 1)
           WHERE sink = OLD.sink;
        END;

        DELETE FROM outbox_queue_counts;
        INSERT INTO outbox_queue_counts (sink, queued)
        SELECT sink, COUNT(*)
          FROM outbox_jobs
         WHERE state != 'delivered'
         GROUP BY sink;
      `);
    });
  }

  addColumnIfMissing(table, column, definition) {
    const columns = this.db.prepare(`PRAGMA table_info("${table}")`).all();
    if (!columns.some((item) => item.name === column)) {
      this.db.exec(
        `ALTER TABLE "${table}" ADD COLUMN "${column}" ${definition}`
      );
    }
  }

  checkIntegrity({ sqlite = true, full = false } = {}) {
    const requiredTables = [
      "outbox_jobs",
      "dead_letter_jobs",
      "outbox_sink_controls",
      "outbox_queue_counts",
    ];
    const requiredTriggers = [
      "outbox_queue_count_insert",
      "outbox_queue_count_deliver",
      "outbox_queue_count_reactivate",
      "outbox_queue_count_delete",
    ];
    const tableNames = new Set(
      this.db
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all()
        .map(({ name }) => name)
    );
    const triggerNames = new Set(
      this.db
        .prepare("SELECT name FROM sqlite_master WHERE type = 'trigger'")
        .all()
        .map(({ name }) => name)
    );
    const missingTables = requiredTables.filter((name) => !tableNames.has(name));
    const missingTriggers = requiredTriggers.filter(
      (name) => !triggerNames.has(name)
    );
    const canCompareCounters =
      tableNames.has("outbox_jobs") && tableNames.has("outbox_queue_counts");
    const actualCounts = canCompareCounters
      ? this.db
          .prepare(
            `SELECT sink, COUNT(*) AS queued
               FROM outbox_jobs
              WHERE state != 'delivered'
              GROUP BY sink`
          )
          .all()
      : [];
    const trackedCounts = canCompareCounters
      ? this.db
          .prepare(
            "SELECT sink, queued FROM outbox_queue_counts WHERE queued != 0"
          )
          .all()
      : [];
    const normalizeCounts = (rows) =>
      JSON.stringify(
        rows
          .map(({ sink, queued }) => [sink, Number(queued)])
          .sort(([left], [right]) => left.localeCompare(right))
      );
    const countersMatch =
      canCompareCounters &&
      normalizeCounts(actualCounts) === normalizeCounts(trackedCounts);
    let sqliteMessages = [];
    if (sqlite) {
      const pragma = full ? "integrity_check" : "quick_check";
      sqliteMessages = this.db
        .prepare(`PRAGMA ${pragma}`)
        .all()
        .map((row) => String(Object.values(row)[0]));
    }
    const sqliteOk =
      !sqlite || (
        sqliteMessages.length === 1 &&
        sqliteMessages[0].toLowerCase() === "ok"
      );
    const errors = [
      ...missingTables.map((name) => `missing table ${name}`),
      ...missingTriggers.map((name) => `missing trigger ${name}`),
      ...(countersMatch ? [] : ["queue counters do not match active jobs"]),
      ...(sqliteOk ? [] : sqliteMessages),
    ];
    return {
      ok: errors.length === 0,
      mode: sqlite ? (full ? "full" : "quick") : "structural",
      missingTables,
      missingTriggers,
      countersMatch,
      sqliteMessages,
      errors,
    };
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

  readDiskFreeBytes({ refresh = false } = {}) {
    if (this.filename === ":memory:") return null;
    const now = Date.now();
    if (
      !refresh &&
      this.cachedDiskSpace &&
      now - this.cachedDiskSpace.checkedAt < this.storageCheckIntervalMs
    ) {
      return this.cachedDiskSpace.freeBytes;
    }
    let freeBytes;
    if (typeof this.diskFreeBytesProvider === "function") {
      freeBytes = Number(this.diskFreeBytesProvider());
    } else if (this.diskFreeBytesProvider != null) {
      freeBytes = Number(this.diskFreeBytesProvider);
    } else {
      const filesystem = statfsSync(dirname(this.filename));
      freeBytes = Number(filesystem.bavail) * Number(filesystem.bsize);
    }
    if (!Number.isFinite(freeBytes) || freeBytes < 0) {
      throw new Error("Unable to determine free disk space for the outbox");
    }
    this.cachedDiskSpace = { freeBytes, checkedAt: now };
    return freeBytes;
  }

  storageHealth({ refreshDisk = false } = {}) {
    const pageSize = Number(this.db.prepare("PRAGMA page_size").get().page_size);
    const pageCount = Number(
      this.db.prepare("PRAGMA page_count").get().page_count
    );
    const freelistPages = Number(
      this.db.prepare("PRAGMA freelist_count").get().freelist_count
    );
    const reusableBytes = freelistPages * pageSize;
    const usedDatabaseBytes = Math.max(
      0,
      (pageCount - freelistPages) * pageSize
    );
    const filesystemFreeBytes = this.readDiskFreeBytes({
      refresh: refreshDisk,
    });
    return {
      pageSize,
      pageCount,
      freelistPages,
      reusableBytes,
      usedDatabaseBytes,
      maxDatabaseBytes: this.maxDatabaseBytes,
      databaseCapacityAvailableBytes: Math.max(
        0,
        this.maxDatabaseBytes - usedDatabaseBytes
      ),
      filesystemFreeBytes,
      minFreeDiskBytes: this.minFreeDiskBytes,
      diskHeadroomAvailableBytes:
        filesystemFreeBytes == null
          ? null
          : Math.max(0, filesystemFreeBytes - this.minFreeDiskBytes),
    };
  }

  assertStorageCapacity(additionalBytes) {
    const requestedBytes = Math.max(0, Number(additionalBytes) || 0);
    const storage = this.storageHealth();
    const projectedUsedBytes = storage.usedDatabaseBytes + requestedBytes;
    if (projectedUsedBytes > this.maxDatabaseBytes) {
      throw capacityError(
        "OUTBOX_DATABASE_CAPACITY",
        `Outbox database usage would exceed the ${this.maxDatabaseBytes} byte limit`,
        { requestedBytes, projectedUsedBytes }
      );
    }
    if (storage.filesystemFreeBytes != null) {
      const growthBytes = Math.max(0, requestedBytes - storage.reusableBytes);
      if (
        storage.filesystemFreeBytes - growthBytes <
        this.minFreeDiskBytes
      ) {
        throw capacityError(
          "OUTBOX_DISK_HEADROOM",
          `Outbox filesystem would fall below the ${this.minFreeDiskBytes} byte free-space reserve`,
          {
            requestedBytes,
            growthBytes,
            filesystemFreeBytes: storage.filesystemFreeBytes,
          }
        );
      }
    }
    return storage;
  }

  assertQueueCapacity(additional) {
    const queued = this.db
      .prepare(
        "SELECT COALESCE(SUM(queued), 0) AS count FROM outbox_queue_counts"
      )
      .get().count;
    if (queued + additional > this.maxQueuedJobs) {
      throw capacityError(
        "OUTBOX_QUEUE_CAPACITY",
        `Outbox queue exceeds the ${this.maxQueuedJobs} job limit`,
        { queued, additional }
      );
    }
  }

  /**
   * Atomically insert one or more jobs with SHA-256 deduplication.
   * @param {Object|Object[]} jobs - A single job definition or an array of them.
   * Each job requires `sink` (string) and `payload` (any safe value).
   * Optional: `dedupeKey`, `maxAttempts`, `retryUntilExpired`,
   * `maxAgeMs`, `baseDelayMs`, `maxDelayMs`, `availableAt`.
   * @returns {Array<{id: string, dedupeKey: string, inserted: boolean, state: string}>}
   */
  enqueue(jobs) {
    const input = Array.isArray(jobs) ? jobs : [jobs];
    if (!input.length) return [];
    if (input.length > this.maxEnqueueBatch) {
      throw new RangeError(
        `Outbox enqueue batch exceeds the ${this.maxEnqueueBatch} job limit`
      );
    }
    const now = this.now();
    const prepared = input.map((job) => {
      if (!job || typeof job !== "object") {
        throw new TypeError("Every outbox job must be an object");
      }
      if (Object.prototype.hasOwnProperty.call(job, "id")) {
        const error = new TypeError(
          "Outbox job ids are managed internally; use dedupeKey for idempotency"
        );
        error.code = "OUTBOX_MANAGED_ID";
        throw error;
      }
      if (!job.sink || typeof job.sink !== "string") {
        throw new TypeError("Every outbox job requires a string sink");
      }
      if (!Object.prototype.hasOwnProperty.call(job, "payload")) {
        throw new TypeError("Every outbox job requires a payload");
      }

      const { payloadJson, dedupeString } = serializePayload(job.payload);
      const payloadBytes = Buffer.byteLength(payloadJson, "utf8");
      if (payloadBytes > this.maxJobBytes) {
        throw new RangeError(
          `Outbox payload exceeds the ${this.maxJobBytes} byte limit`
        );
      }
      const dedupeKey =
        job.dedupeKey ||
        createHash("sha256")
          .update(`${job.sink}\n${dedupeString}`)
          .digest("hex");
      return { job, payloadJson, payloadBytes, dedupeKey };
    });
    const insert = this.db.prepare(`
      INSERT INTO outbox_jobs (
        id, dedupe_key, sink, payload_json, payload_encoding,
        max_attempts, retry_until_expired, max_age_ms,
        base_delay_ms, max_delay_ms,
        available_at, created_at
      ) VALUES (?, ?, ?, ?, 'canonical-v2', ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(dedupe_key) DO NOTHING
    `);
    const existing = this.db.prepare(
      "SELECT id, state FROM outbox_jobs WHERE dedupe_key = ?"
    );
    const existingDead = this.db.prepare(
      "SELECT id, 'dead' AS state FROM dead_letter_jobs WHERE dedupe_key = ?"
    );
    const estimatedKeys = new Set();
    const estimatedBytes = prepared.reduce((total, item) => {
      if (
        estimatedKeys.has(item.dedupeKey) ||
        existing.get(item.dedupeKey)
      ) {
        return total;
      }
      estimatedKeys.add(item.dedupeKey);
      return total + item.payloadBytes + 1_024;
    }, 0);
    const storage = this.assertStorageCapacity(estimatedBytes);
    const estimatedGrowthBytes = Math.max(
      0,
      estimatedBytes - storage.reusableBytes
    );

    const results = this.transaction(() => {
      let queued = this.db
        .prepare(
          "SELECT COALESCE(SUM(queued), 0) AS count FROM outbox_queue_counts"
        )
        .get().count;
      return prepared.map(({ job, payloadJson, dedupeKey }) => {
        const id = randomUUID();
        const result = insert.run(
          id,
          dedupeKey,
          job.sink,
          payloadJson,
          job.maxAttempts ?? D.MAX_ATTEMPTS,
          job.retryUntilExpired ? 1 : 0,
          job.maxAgeMs ?? D.MAX_AGE_MS,
          job.baseDelayMs ?? D.BASE_DELAY_MS,
          job.maxDelayMs ?? D.MAX_DELAY_MS,
          job.availableAt ?? now,
          now
        );
        if (result.changes === 1) {
          queued += 1;
          if (queued > this.maxQueuedJobs) {
            throw capacityError(
              "OUTBOX_QUEUE_CAPACITY",
              `Outbox queue exceeds the ${this.maxQueuedJobs} job limit`,
              { queued: queued - 1, additional: 1 }
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
    if (this.cachedDiskSpace && estimatedGrowthBytes > 0) {
      this.cachedDiskSpace.freeBytes = Math.max(
        0,
        this.cachedDiskSpace.freeBytes - estimatedGrowthBytes
      );
    }
    return results;
  }

  /**
   * Lease the single oldest ready job for a sink.
   * Convenience wrapper around claimBatch with batchSize=1.
   * @param {{sink: string, leaseMs?: number, maxInFlight?: number, circuitBreakerEnabled?: boolean}} options
   * @returns {Object|null} A parsed job with `payload`, `id`, `leaseToken`, or null.
   */
  claim(options = {}) {
    return this.claimBatch({ ...options, batchSize: 1 })[0] || null;
  }

  /**
   * Lease a batch of ready jobs for a sink, respecting in-flight limits.
   * Handles circuit-breaker pauses and exhausted-job dead-lettering.
   * @param {{sink: string, leaseMs?: number, maxInFlight?: number, batchSize?: number, circuitBreakerEnabled?: boolean}} options
   * @returns {Array<Object>} Array of parsed jobs with `payload`, `id`, `leaseToken`.
   */
  claimBatch({
    sink,
    leaseMs = D.LEASE_MS,
    maxInFlight = D.MAX_IN_FLIGHT,
    batchSize = D.BATCH_SIZE,
    circuitBreakerEnabled = true,
  } = {}) {
    if (!sink) throw new Error("A sink is required to claim an outbox job");
    const now = this.now();
    const safeMaxInFlight = Math.max(1, Number(maxInFlight) || D.MAX_IN_FLIGHT);
    const safeBatchSize = Math.max(
      1,
      Math.min(1_000, Number(batchSize) || D.BATCH_SIZE)
    );
    const safeLeaseMs = Math.max(1, Number(leaseMs) || D.LEASE_MS);

    try {
      return this.transaction(() => {
      const control = this.getSinkControl(sink);
      if (
        control.manuallyPaused ||
        (circuitBreakerEnabled &&
          control.pausedUntil != null &&
          control.pausedUntil > now)
      ) {
        return [];
      }

      let sweptExhausted = false;
      const nextSweep = this.nextExhaustionSweep.get(sink);
      if (nextSweep == null) {
        this.nextExhaustionSweep.set(
          sink,
          now + EXHAUSTION_SWEEP_INTERVAL_MS
        );
      } else if (now >= nextSweep) {
        this._moveExhaustedToDeadLetter(sink, now);
        this.nextExhaustionSweep.set(
          sink,
          now + EXHAUSTION_SWEEP_INTERVAL_MS
        );
        sweptExhausted = true;
      }

      // Half-open: the circuit was open and its cooldown has elapsed, but no
      // success has cleared `paused_until` yet. Lease a single trial job so a
      // still-broken sink faces one probe rather than a full batch of failures.
      const halfOpen =
        circuitBreakerEnabled &&
        control.pausedUntil != null &&
        control.pausedUntil <= now;

      const active = this.db
        .prepare(
          `SELECT COUNT(*) AS count
             FROM outbox_jobs
            WHERE sink = ? AND state = 'leased' AND lease_until > ?`
        )
        .get(sink, now);
      const capacity = Math.min(
        halfOpen ? 1 : safeBatchSize,
        safeMaxInFlight - active.count
      );
      if (capacity <= 0) return [];

      const candidates = this.db
        .prepare(
          `SELECT id
             FROM outbox_jobs
            WHERE sink = ?
              AND available_at <= ?
              AND (state = 'pending' OR (state = 'leased' AND lease_until <= ?))
              AND NOT (
                (retry_until_expired = 0 AND attempts >= max_attempts)
                OR ? - created_at >= max_age_ms
              )
            ORDER BY created_at, id
            LIMIT ?`
        )
        .all(sink, now, now, now, capacity);
      if (!candidates.length) {
        if (!sweptExhausted) {
          this._moveExhaustedToDeadLetter(sink, now);
          this.nextExhaustionSweep.set(
            sink,
            now + EXHAUSTION_SWEEP_INTERVAL_MS
          );
        }
        return [];
      }

      const lease = this.db.prepare(
        `UPDATE outbox_jobs
            SET state = 'leased', attempts = attempts + 1,
                lease_until = ?, lease_token = ?
          WHERE id = ?
          RETURNING *`
      );
      const jobs = [];
      let quarantined = 0;
      for (const candidate of candidates) {
        if (quarantined >= D.MAX_QUARANTINE_PER_CLAIM) break;
        const token = randomUUID();
        const row = lease.get(now + safeLeaseMs, token, candidate.id);
        try {
          jobs.push(this.parseJob(row));
        } catch (error) {
          if (error.code === "OUTBOX_UNSUPPORTED_ENCODING") throw error;
          if (error.code !== "OUTBOX_INVALID_PAYLOAD") throw error;
          this._insertDeadLetter(
            row,
            serializeError(error),
            null,
            "outbox-corruption",
            now
          );
          this.db.prepare("DELETE FROM outbox_jobs WHERE id = ?").run(row.id);
          quarantined += 1;
        }
      }
      jobs.quarantined = quarantined;
      return jobs;
      });
    } catch (error) {
      if (error.code === "OUTBOX_UNSUPPORTED_ENCODING") {
        this.pauseSink(sink, { error });
        error.sink = sink;
        error.sinkPaused = true;
      }
      throw error;
    }
  }

  _moveExhaustedToDeadLetter(sink, now) {
    const exhausted = this.db
      .prepare(
        `SELECT *
           FROM outbox_jobs
          WHERE sink = ?
            AND state != 'delivered'
            AND (state = 'pending' OR (state = 'leased' AND lease_until <= ?))
            AND ((retry_until_expired = 0 AND attempts >= max_attempts)
              OR ? - created_at >= max_age_ms)
          ORDER BY created_at, id
          LIMIT ?`
      )
      .all(sink, now, now, EXHAUSTION_SWEEP_LIMIT);

    if (!exhausted.length) return 0;

    const insertDead = this.db.prepare(
      `INSERT INTO dead_letter_jobs (${DEAD_INSERT_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    const deleteJob = this.db.prepare("DELETE FROM outbox_jobs WHERE id = ?");

    for (const row of exhausted) {
      const reason =
        row.attempts >= row.max_attempts
          ? "Lease expired after maximum delivery attempts"
          : "Job exceeded maximum delivery age";
      insertDead.run(
        row.id, row.dedupe_key, row.sink,
        row.payload_json, row.payload_encoding,
        row.attempts, row.max_attempts, row.retry_until_expired,
        row.max_age_ms, row.base_delay_ms, row.max_delay_ms,
        row.first_error || reason, reason,
        row.last_status,
        row.last_failure_class || "infrastructure",
        row.created_at, now
      );
      deleteJob.run(row.id);
    }
    return exhausted.length;
  }

  /**
   * Move attempt-exhausted and age-expired jobs to the dead-letter table for
   * one sink or, by default, every sink with active jobs. Unlike the sweep
   * folded into `claimBatch`, this runs regardless of manual pauses or open
   * circuits, so expiry does not depend on a claim node polling.
   * @param {{sink?: string}} options
   * @returns {{swept: number, sinks: number}}
   */
  sweepExhausted({ sink } = {}) {
    const now = this.now();
    return this.transaction(() => {
      const sinks = sink
        ? [sink]
        : this.db
            .prepare("SELECT DISTINCT sink FROM outbox_jobs")
            .all()
            .map((row) => row.sink);
      let swept = 0;
      for (const target of sinks) {
        swept += this._moveExhaustedToDeadLetter(target, now);
        this.nextExhaustionSweep.set(
          target,
          now + EXHAUSTION_SWEEP_INTERVAL_MS
        );
      }
      return { swept, sinks: sinks.length };
    });
  }

  /**
   * Mark a leased job as delivered, schedule a retry, or move to dead-letter table.
   * Lease-token fencing prevents stale workers from settling.
   * @param {string} id - The job ID from claim/claimBatch.
   * @param {{leaseToken: string, success?: boolean, retryable?: boolean,
   *   error?: Error|string, status?: number, failureClass?: string,
   *   circuitFailure?: boolean, circuitBreakerThreshold?: number,
   *   circuitBreakerCooldownMs?: number}} outcome
   * @returns {{id: string, state: 'delivered'|'pending'|'dead'|'stale_lease', ...}}
   */
  settle(id, outcome = {}) {
    this._validateSettlement(id, outcome);
    const now = this.now();
    return this.transaction(() => this._settle(id, outcome, now));
  }

  /**
   * Settle a group of leases in one SQLite transaction.
   * @param {Array<{id: string, outcome: Object}>} settlements
   * @param {{circuitFailureOnce?: boolean}} options
   * @returns {Array<Object>} Settlement results in input order.
   */
  settleBatch(settlements, { circuitFailureOnce = false } = {}) {
    if (!Array.isArray(settlements) || !settlements.length) {
      throw new Error("At least one outbox settlement is required");
    }
    for (const settlement of settlements) {
      this._validateSettlement(settlement?.id, settlement?.outcome);
    }
    const now = this.now();
    return this.transaction(() => {
      let circuitFailurePending =
        circuitFailureOnce &&
        settlements.some(({ outcome }) => outcome.circuitFailure);
      return settlements.map(({ id, outcome }) => {
        const effectiveOutcome = circuitFailureOnce
          ? { ...outcome, circuitFailure: circuitFailurePending }
          : outcome;
        const result = this._settle(id, effectiveOutcome, now);
        if (
          circuitFailurePending &&
          result.state !== "stale_lease"
        ) {
          circuitFailurePending = false;
        }
        return result;
      });
    });
  }

  _validateSettlement(id, outcome = {}) {
    if (!id) throw new Error("An outbox job id is required");
    if (!outcome?.leaseToken || typeof outcome.leaseToken !== "string") {
      throw new Error("A lease token is required to settle an outbox job");
    }
  }

  _settle(id, outcome, now) {
    const row = this.db
      .prepare("SELECT * FROM outbox_jobs WHERE id = ?")
      .get(id);
    if (!row) throw new Error(`Outbox job ${id} does not exist`);
    if (row.state !== "leased" || row.lease_token !== outcome.leaseToken) {
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
    const circuitFailure =
      outcome.circuitFailure ?? failureClass === "infrastructure";
    const dead =
      outcome.retryable === false ||
      now - row.created_at >= row.max_age_ms ||
      (!row.retry_until_expired && row.attempts >= row.max_attempts);

    const circuit = this.recordSinkFailure(row.sink, {
      now,
      errorText,
      circuitFailure,
      threshold: outcome.circuitBreakerThreshold,
      cooldownMs: outcome.circuitBreakerCooldownMs,
    });

    if (dead) {
      this._insertDeadLetter(row, errorText, status, failureClass, now);
      this.db.prepare("DELETE FROM outbox_jobs WHERE id = ?").run(id);
      return {
        id,
        state: "dead",
        attempts: row.attempts,
        reason: outcome.retryable === false ? "non-retryable"
          : now - row.created_at >= row.max_age_ms ? "expired"
          : "attempts-exhausted",
        circuit,
      };
    }

    return this._scheduleRetry(
      row,
      errorText,
      status,
      failureClass,
      now,
      id,
      outcome.leaseToken,
      circuit
    );
  }

  _insertDeadLetter(row, errorText, status, failureClass, now) {
    this.db
      .prepare(
        `INSERT INTO dead_letter_jobs (${DEAD_INSERT_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        row.id, row.dedupe_key, row.sink,
        row.payload_json, row.payload_encoding,
        row.attempts, row.max_attempts, row.retry_until_expired,
        row.max_age_ms, row.base_delay_ms, row.max_delay_ms,
        row.first_error || errorText, errorText,
        status, failureClass,
        row.created_at, now
      );
  }

  _scheduleRetry(row, errorText, status, failureClass, now, id, leaseToken, circuit) {
    const ceiling = Math.min(
      row.max_delay_ms,
      row.base_delay_ms * 2 ** Math.max(0, row.attempts - 1)
    );
    const delayMs = Math.floor(this.random() * ceiling);
    const availableAt = now + delayMs;
    this.db
      .prepare(
        `UPDATE outbox_jobs
            SET state = 'pending', available_at = ?,
                lease_until = NULL, lease_token = NULL,
                first_error = COALESCE(first_error, ?),
                last_error = ?, last_status = ?,
                last_failure_class = ?
          WHERE id = ? AND lease_token = ?`
      )
      .run(availableAt, errorText, errorText, status, failureClass, id, leaseToken);
    return { id, state: "pending", attempts: row.attempts, delayMs, availableAt, circuit };
  }

  /**
   * Return circuit-breaker and manual-pause state for a sink.
   * @param {string} sink
   * @returns {{sink: string, manuallyPaused: boolean, pausedUntil: number|null,
   *   consecutiveFailures: number, lastFailureAt: number|null,
   *   lastSuccessAt: number|null, lastError: string|null}}
   */
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
      circuitFailure,
      threshold = D.CIRCUIT_THRESHOLD,
      cooldownMs = D.CIRCUIT_COOLDOWN_MS,
    } = {}
  ) {
    if (!circuitFailure) {
      return this.getSinkControl(sink);
    }
    const limit = Math.max(1, Number(threshold) || D.CIRCUIT_THRESHOLD);
    const cooldown = Math.max(1_000, Number(cooldownMs) || D.CIRCUIT_COOLDOWN_MS);
    this.db
      .prepare(
        `INSERT INTO outbox_sink_controls (
          sink, consecutive_failures, last_failure_at, last_error
        ) VALUES (?, 1, ?, ?)
        ON CONFLICT(sink) DO UPDATE SET
          consecutive_failures = MIN(consecutive_failures + 1, ?),
          last_failure_at = excluded.last_failure_at,
          last_error = excluded.last_error`
      )
      .run(sink, now, errorText, limit);
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

  /**
   * Manually pause claims for a sink. Existing leases are unaffected.
   * @param {string} sink
   * @returns {Object} Updated sink control state.
   */
  pauseSink(sink, { error } = {}) {
    const errorText = error == null ? null : serializeError(error);
    this.db
      .prepare(
        `INSERT INTO outbox_sink_controls (sink, manually_paused, last_error)
         VALUES (?, 1, ?)
         ON CONFLICT(sink) DO UPDATE SET
           manually_paused = 1,
           last_error = COALESCE(excluded.last_error, last_error)`
      )
      .run(sink, errorText);
    return this.getSinkControl(sink);
  }

  /**
   * Resume a manually paused sink, clear its circuit breaker, and optionally
   * make all pending jobs immediately eligible.
   * @param {string} sink
   * @param {{retryNow?: boolean}} options
   * @returns {{control: Object, released: number}}
   */
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

  /**
   * Make all pending jobs for a sink immediately claimable without clearing
   * manual pause or circuit state.
   * @param {string} sink
   * @returns {{sink: string, released: number, availableAt: number}}
   */
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

  /**
   * List dead-letter jobs with optional sink and failure-class filtering.
   * @param {{sink?: string, failureClass?: string, limit?: number}} options
   * @returns {Array<Object>} Parsed dead-letter jobs ordered by failed_at DESC.
   */
  listDeadLetters(options = {}) {
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
      .map((row) => this.parseJob(row, { tolerateInvalidPayload: true }));
  }

  /**
   * Atomically move one dead letter back to the pending queue (resets attempts).
   * @param {string} id - The dead-letter job ID.
   * @returns {{id: string, state: 'pending', availableAt: number}}
   */
  requeueDeadLetter(id) {
    const now = this.now();
    return this.transaction(() => {
      const row = this.db
        .prepare("SELECT * FROM dead_letter_jobs WHERE id = ?")
        .get(id);
      if (!row) throw new Error(`Dead-letter job ${id} does not exist`);
      if (row.last_failure_class === "outbox-corruption") {
        const error = new Error(
          `Dead-letter job ${id} contains a quarantined payload and cannot be replayed`
        );
        error.code = "OUTBOX_CORRUPT_REPLAY";
        throw error;
      }
      this.assertQueueCapacity(1);
      this.db
        .prepare(
          `INSERT INTO outbox_jobs (${JOB_INSERT_COLUMNS}) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          row.id,
          row.dedupe_key,
          row.sink,
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

  /**
   * Atomically move matching dead letters back to the pending queue.
   * Filters by sink, failure class, and limit.
   * @param {{sink?: string, failureClass?: string, limit?: number}} options
   * @returns {{requeued: number, ids: string[], sink: string|null, failureClass: string|null}}
   */
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
    const corruptWhere = [...clauses, "last_failure_class = 'outbox-corruption'"];
    const corruptParams = [...params];
    clauses.push("last_failure_class != 'outbox-corruption'");
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const safeLimit = Math.max(1, Math.min(10_000, Number(limit) || 1_000));

    return this.transaction(() => {
      const corruptSkipped = this.db
        .prepare(
          `SELECT COUNT(*) AS count FROM dead_letter_jobs
           WHERE ${corruptWhere.join(" AND ")}`
        )
        .get(...corruptParams).count;
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
        `INSERT INTO outbox_jobs (${JOB_INSERT_COLUMNS}) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?)`
      );
      const remove = this.db.prepare(
        "DELETE FROM dead_letter_jobs WHERE id = ?"
      );
      for (const row of rows) {
        insert.run(
          row.id,
          row.dedupe_key,
          row.sink,
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
        corruptSkipped,
      };
    });
  }

  /**
   * Permanently delete dead letters matching optional sink/failure-class filters.
   * @param {{sink?: string, failureClass?: string, limit?: number}} options
   * @returns {{deleted: number, ids: string[], sink: string|null, failureClass: string|null}}
   */
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

  /**
   * Delete delivered records older than the given threshold, bounded by limit.
   * @param {{olderThanMs?: number, limit?: number}} options
   * @returns {{purged: number, cutoff: number, olderThanMs: number}}
   */
  purgeDelivered({ olderThanMs = D.MAX_AGE_MS, limit = 1_000 } = {}) {
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

  /**
   * Expire exhausted jobs, WAL checkpoint, and optional VACUUM.
   * @param {{sweep?: boolean, checkpoint?: boolean, vacuum?: boolean}} options
   * @returns {{sweep: Object|null, checkpoint: Object|null, vacuumed: boolean}}
   */
  maintenance({ sweep = true, checkpoint = true, vacuum = false } = {}) {
    const result = { sweep: null, checkpoint: null, vacuumed: false };
    if (sweep) {
      result.sweep = this.sweepExhausted();
    }
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

  countQueued(sink) {
    if (!sink) throw new Error("A sink is required to count queued jobs");
    return (
      this.db
        .prepare(
          "SELECT queued FROM outbox_queue_counts WHERE sink = ?"
        )
        .get(sink)?.queued || 0
    );
  }

  /**
   * Return comprehensive health statistics: jobs by sink/state, dead-letter count,
   * per-sink control state, queue depth, oldest queued age, expired leases,
   * and database size.
   * @returns {{jobs: Array, deadLetters: number, controls: Array, health: Object}}
   */
  stats() {
    const now = this.now();
    const rows = this.db
      .prepare(
        `SELECT sink, state, COUNT(*) AS count
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
    const storage = this.storageHealth();
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
        ...storage,
        startupIntegrity: this.startupIntegrity,
        databaseSizeWarning:
          storage.usedDatabaseBytes >= this.maxDatabaseBytes * 0.9,
      },
    };
  }

  /**
   * Fetch a single active job by ID.
   * @param {string} id
   * @returns {Object|null} Parsed job or null if not found.
   */
  getJob(id) {
    const row = this.db
      .prepare("SELECT * FROM outbox_jobs WHERE id = ?")
      .get(id);
    return row ? this.parseJob(row) : null;
  }

  parseJob(row, { tolerateInvalidPayload = false } = {}) {
    let payload;
    let payloadDecodeError = null;
    try {
      payload = decodePayload(row.payload_json, row.payload_encoding);
    } catch (error) {
      if (!tolerateInvalidPayload) throw error;
      payload = null;
      payloadDecodeError = {
        code: error.code || "OUTBOX_INVALID_PAYLOAD",
        message: error.message,
      };
    }
    return {
      id: row.id,
      dedupeKey: row.dedupe_key,
      sink: row.sink,
      payload,
      payloadDecodeError,
      payloadRaw: payloadDecodeError == null ? undefined : row.payload_json,
      replayable: payloadDecodeError == null,
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

  /**
   * Close the SQLite database connection.
   */
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
