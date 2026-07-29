"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const flowPath = path.join(__dirname, "..", "demo", "flows_nosql.json");
const flow = JSON.parse(fs.readFileSync(flowPath, "utf8"));
const mainFlow = JSON.parse(
    fs.readFileSync(path.join(__dirname, "..", "demo", "flows.json"), "utf8")
);
const byId = new Map(flow.map(node => [node.id, node]));
const findNode = (type, name) =>
    flow.find(node => node.type === type && node.name === name);

function assertStructuralReferences(nodes) {
    const nodesById = new Map(nodes.map(node => [node.id, node]));
    assert.equal(nodesById.size, nodes.length, "node ids must be unique");
    for (const node of nodes) {
        if (node.z) {
            assert.ok(nodesById.has(node.z), `${node.id} has a missing flow reference`);
        }
        if (node.g) {
            assert.ok(nodesById.has(node.g), `${node.id} has a missing group reference`);
        }
        for (const target of (node.wires || []).flat()) {
            assert.ok(nodesById.has(target), `${node.id} wires to missing node ${target}`);
        }
        for (const target of node.scope || []) {
            assert.ok(nodesById.has(target), `${node.id} scopes missing node ${target}`);
        }
    }
}

test("No-SQL demo is a self-contained, structurally valid Node-RED flow", () => {
    assertStructuralReferences(flow);

    for (const node of flow.filter(candidate => candidate.type === "function")) {
        assert.doesNotThrow(
            () => new Function("msg", "node", node.func),
            `${node.name} contains invalid JavaScript`
        );
    }
});

test("main demo remains structurally valid", () => {
    assertStructuralReferences(mainFlow);
});

test("No-SQL worker uses batch claims and partitions leases with their plans", () => {
    const claim = findNode("outbox-claim", "Claim plans");
    const compiler = findNode("function", "Validate + compile registry");

    assert.ok(claim);
    assert.ok(compiler);
    assert.equal(claim.outputMode, "batch");
    assert.equal(compiler.outputs, 2);

    const validPlan = {
        version: 1,
        eventId: "evt-valid",
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
    const invalidPlan = {
        version: 1,
        eventId: "evt-invalid",
        writes: {
            sensor_readings: [{
                device_id: "sensor-2",
                observed_at: "2026-07-28T12:00:01.000Z"
            }]
        }
    };
    const leases = [
        { id: "lease-valid", token: "token-valid" },
        { id: "lease-invalid", token: "token-invalid" }
    ];
    // Node-RED executes Function nodes in a VM realm distinct from the realm
    // that creates messages. Reproduce that cross-realm prototype boundary.
    const decodedValidPlan = vm.runInNewContext(
        "JSON.parse(input)",
        { input: JSON.stringify(validPlan) }
    );
    const decodedInvalidPlan = vm.runInNewContext(
        "JSON.parse(input)",
        { input: JSON.stringify(invalidPlan) }
    );
    const compile = new Function("msg", "node", compiler.func);
    const [validMessage, invalidMessage] = compile({
        payload: [decodedValidPlan, decodedInvalidPlan],
        outbox: { batch: leases }
    }, { warn() {} });

    assert.deepEqual(validMessage.payload, [decodedValidPlan]);
    assert.deepEqual(validMessage.outbox.batch, [leases[0]]);
    assert.match(validMessage.query, /WITH\s+input\(doc\)/);
    assert.match(validMessage.query, /INSERT INTO "sensor_readings"/);
    assert.match(validMessage.query, /INSERT INTO "sensor_health"/);
    assert.match(validMessage.query, /"observed_at" timestamptz/);
    assert.match(validMessage.query, /UPDATE SET "value_mm" = EXCLUDED\."value_mm"/);
    assert.doesNotMatch(validMessage.query, /"device_id" = EXCLUDED\."device_id"/);
    assert.equal(validMessage.params.length, 1);
    assert.equal(validMessage.outbox.planVersion, 1);
    assert.deepEqual(
        Object.keys(JSON.parse(validMessage.params[0])).sort(),
        ["errors", "sensor_health", "sensor_readings"]
    );

    assert.deepEqual(invalidMessage.payload, [decodedInvalidPlan]);
    assert.deepEqual(invalidMessage.outbox.batch, [leases[1]]);
    assert.equal(invalidMessage.outbox.retryable, false);
    assert.equal(invalidMessage.outbox.circuitFailure, false);
    assert.equal(invalidMessage.outbox.failureClass, "data");
    assert.match(invalidMessage.outbox.validationErrors[0].error, /value_mm is required/);
});

test("No-SQL registry owns types, update policy, and the durable sink version", () => {
    const subflow = flow.find(node => node.type === "subflow" &&
        node.name === "No-SQL Outbox");
    const sink = findNode("outbox-sink-config", "PostgreSQL write plans");
    const compiler = findNode("function", "Validate + compile registry");

    assert.ok(subflow);
    assert.ok(sink);
    assert.ok(compiler);
    assert.deepEqual(
        subflow.env.map(property => property.name).sort(),
        ["OUTBOX_SINK", "POSTGRES_CONFIG"]
    );
    assert.match(sink.sinkKey, /-v1$/);
    assert.match(compiler.func, /timestamptz:\s*\{/);
    assert.match(compiler.func, /update:\s*\["value_mm"\]/);
    assert.match(compiler.func, /optional-column semantics are not defined/);

    const compile = new Function("msg", "node", compiler.func);
    const wrongTypePlan = vm.runInNewContext("JSON.parse(input)", {
        input: JSON.stringify({
            version: 1,
            eventId: "evt-wrong-type",
            writes: {
                sensor_readings: [{
                    device_id: "sensor-3",
                    observed_at: "not-a-timestamp",
                    value_mm: 12.5
                }]
            }
        })
    });
    const [validMessage, invalidMessage] = compile({
        payload: [wrongTypePlan],
        outbox: { batch: [{ id: "lease-wrong-type" }] }
    }, { warn() {} });

    assert.equal(validMessage, null);
    assert.equal(invalidMessage.outbox.failureClass, "data");
    assert.match(
        invalidMessage.outbox.validationErrors[0].error,
        /observed_at must be timestamptz/
    );
});

test("durable error logger emits a safe idempotent WritePlan", () => {
    const logger = flow.find(node => node.type === "subflow" &&
        node.name === "Log Error → Durable WritePlan");
    const formatter = findNode("function", "Build Error WritePlan v1");
    const enqueue = findNode("outbox-enqueue", "Persist error plan");
    const compiler = findNode("function", "Validate + compile registry");
    const schema = findNode("postgresql", "CREATE TABLE IF NOT EXISTS");

    assert.ok(logger);
    assert.ok(formatter);
    assert.equal(enqueue.sink, "${OUTBOX_SINK}");
    assert.match(schema.query, /UNIQUE \(event_id, time\)/);

    const cause = new Error("socket timeout");
    const caughtMessage = {
        _msgid: "message-123",
        timestamp: Date.parse("2026-07-29T12:00:00.000Z"),
        dev_id: "sensor-4",
        deployment_id: "deployment-1",
        error: {
            message: "PostgreSQL request failed",
            source: {
                id: "postgres-node",
                type: "postgresql",
                name: "Insert readings",
                count: 1
            },
            cause
        },
        originalSensorPayload: { value: 42 }
    };
    cause.cause = caughtMessage.error;

    const format = new Function("msg", "node", formatter.func);
    const [planMessage, originalMessage] = format(caughtMessage, { warn() {} });
    const [samePlanMessage] = format({ ...caughtMessage }, { warn() {} });
    const plan = planMessage.payload;
    const row = plan.writes.errors[0];

    assert.equal(originalMessage, caughtMessage);
    assert.equal(plan.version, 1);
    assert.equal(plan.eventId, samePlanMessage.payload.eventId);
    assert.equal(row.event_id, plan.eventId);
    assert.equal(row.time, "2026-07-29T12:00:00.000Z");
    assert.equal(row.data.error.cause.message, "socket timeout");
    assert.equal(row.data.error.cause.cause, "[Circular]");
    assert.doesNotThrow(() => JSON.stringify(plan));
    assert.equal(
        planMessage.outbox.dedupeKey,
        "postgres-error:v1:" + plan.eventId
    );

    const compile = new Function("msg", "node", compiler.func);
    const lease = { id: "error-lease", token: "error-token" };
    const [compiled, invalid] = compile({
        payload: [vm.runInNewContext("JSON.parse(input)", {
            input: JSON.stringify(plan)
        })],
        outbox: { batch: [lease] }
    }, { warn() {} });

    assert.equal(invalid, null);
    assert.deepEqual(compiled.outbox.batch, [lease]);
    assert.match(compiled.query, /INSERT INTO "errors"/);
    assert.match(compiled.query, /ON CONFLICT \("event_id", "time"\) DO NOTHING/);
    assert.doesNotMatch(compiled.query, /INSERT INTO "sensor_readings"/);
    assert.doesNotMatch(compiled.query, /INSERT INTO "sensor_health"/);
});

test("error logger instances enqueue without multiplying No-SQL workers", () => {
    const noSqlSubflow = flow.find(node => node.type === "subflow" &&
        node.name === "No-SQL Outbox");
    const loggerSubflow = flow.find(node => node.type === "subflow" &&
        node.name === "Log Error → Durable WritePlan");
    const workers = flow.filter(node =>
        node.type === `subflow:${noSqlSubflow.id}`
    );
    const loggerChildren = flow.filter(node => node.z === loggerSubflow.id);

    assert.equal(workers.length, 1);
    assert.equal(
        loggerChildren.filter(node => node.type === "outbox-enqueue").length,
        1
    );
    assert.equal(
        loggerChildren.some(node => node.type === `subflow:${noSqlSubflow.id}`),
        false
    );
    assert.equal(
        loggerChildren.some(node => node.type === "inject" && node.repeat),
        false
    );
});

test("demo outboxes own cleanup and expose enqueue rejection paths", () => {
    const config = findNode("durable-outbox-config", "No-SQL sample outbox");
    const noSqlSubflow = flow.find(node => node.type === "subflow" &&
        node.name === "No-SQL Outbox");
    const loggerSubflow = flow.find(node => node.type === "subflow" &&
        node.name === "Log Error → Durable WritePlan");

    assert.equal(config.deliveredRetentionMs, 86_400_000);
    assert.equal(config.cleanupIntervalMs, 60_000);
    assert.equal(config.cleanupBatchSize, 10_000);
    assert.equal(config.cleanupHighWatermarkPercent, 80);
    assert.equal(config.cleanupLowWatermarkPercent, 70);
    assert.equal(config.protectIngestion, true);
    assert.equal(noSqlSubflow.out.length, 4);
    assert.equal(noSqlSubflow.outputLabels.at(-1), "not persisted");
    assert.equal(loggerSubflow.out.length, 2);
    assert.equal(loggerSubflow.outputLabels.at(-1), "not persisted");

    const recurringPurges = mainFlow.filter(node =>
        node.type === "inject" &&
        /purge/i.test(node.name || "") &&
        (node.repeat || node.once)
    );
    assert.deepEqual(
        recurringPurges,
        [],
        "manual purge controls must not duplicate automatic cleanup"
    );

    const postgresSinks = mainFlow.filter(node =>
        node.type === "outbox-sink-config" &&
        node.sinkKey?.startsWith("postgres-")
    );
    assert.ok(postgresSinks.length > 0);
    assert.equal(
        postgresSinks.every(node =>
            node.retryUntilExpired === "true" &&
            node.maxAgeMs === 86_400_000
        ),
        true,
        "PostgreSQL demos should retry infrastructure failures until age expiry"
    );
    assert.equal(
        mainFlow
            .filter(node => node.type === "durable-outbox-config")
            .every(node => Number(node.maxJobMb) === 1),
        true
    );
});
