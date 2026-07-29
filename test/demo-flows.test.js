"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const flowPath = path.join(__dirname, "..", "demo", "flows_nosql.json");
const flow = JSON.parse(fs.readFileSync(flowPath, "utf8"));
const byId = new Map(flow.map(node => [node.id, node]));
const findNode = (type, name) =>
    flow.find(node => node.type === type && node.name === name);

test("No-SQL demo is a self-contained, structurally valid Node-RED flow", () => {
    assert.equal(byId.size, flow.length, "node ids must be unique");

    for (const node of flow) {
        if (node.z) {
            assert.ok(byId.has(node.z), `${node.id} has a missing flow reference`);
        }
        if (node.g) {
            assert.ok(byId.has(node.g), `${node.id} has a missing group reference`);
        }
        for (const target of (node.wires || []).flat()) {
            assert.ok(byId.has(target), `${node.id} wires to missing node ${target}`);
        }
        for (const target of node.scope || []) {
            assert.ok(byId.has(target), `${node.id} scopes missing node ${target}`);
        }
    }

    for (const node of flow.filter(candidate => candidate.type === "function")) {
        assert.doesNotThrow(
            () => new Function("msg", "node", node.func),
            `${node.name} contains invalid JavaScript`
        );
    }
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
        ["sensor_health", "sensor_readings"]
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
