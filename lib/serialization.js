"use strict";

const { createHash } = require("node:crypto");

function isPlainObject(value) {
  if (value === null || typeof value !== "object") return false;
  const prototype = Object.getPrototypeOf(value);
  if (prototype === null || prototype === Object.prototype) return true;
  return (
    Object.prototype.toString.call(value) === "[object Object]" &&
    prototype.constructor?.name === "Object"
  );
}

function walk(value, handlers, seen = new Set(), location = "payload") {
  const { onNull, onString, onBoolean, onNumber, onUndefined, onDate, onBuffer, onArray, onObject, onUndefinedArray } = handlers;
  if (value === null) return onNull(value);
  if (typeof value === "string") return onString(value);
  if (typeof value === "boolean") return onBoolean(value);
  if (typeof value === "number") return onNumber(value);

  if (value === undefined) return onUndefined(value);

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
    return onDate(value);
  }

  if (Buffer.isBuffer(value)) {
    return onBuffer(value);
  }

  if (!Array.isArray(value) && !isPlainObject(value)) {
    throw new TypeError(
      `Unsupported object type ${value.constructor?.name || "unknown"} at ${location}`
    );
  }

  seen.add(value);

  let result;
  if (Array.isArray(value)) {
    result = onArray(value.map((item, index) => {
      if (item === undefined) return onUndefinedArray();
      return walk(item, arguments[1], seen, `${location}[${index}]`);
    }));
  } else {
    const entries = Object.keys(value)
      .sort()
      .filter((key) => value[key] !== undefined)
      .map((key) => {
        const child = walk(value[key], arguments[1], seen, `${location}.${key}`);
        return [key, child];
      });
    result = onObject(entries);
  }

  seen.delete(value);
  return result;
}

const CANONICAL_HANDLERS = {
  onNull: () => null,
  onString: (v) => v,
  onBoolean: (v) => v,
  onNumber: (v) => Number.isFinite(v) ? v : null,
  onUndefined: () => undefined,
  onUndefinedArray: () => undefined,
  onDate: (v) => ({ $outboxType: "date", value: v.toISOString() }),
  onBuffer: (v) => ({ $outboxType: "buffer", value: v.toString("base64") }),
  onArray: (children) => children.map((item) => item === undefined ? null : item),
  onObject: (entries) => {
    const result = {};
    for (const [key, val] of entries) {
      if (val !== undefined) result[key] = val;
    }
    return result;
  },
};

const ENCODED_HANDLERS = {
  onNull: () => ["null"],
  onString: (v) => ["string", v],
  onBoolean: (v) => ["boolean", v],
  onNumber: (v) => ["number", Number.isFinite(v) ? v : null],
  onUndefined: () => ["undefined"],
  onUndefinedArray: () => ["null"],
  onDate: (v) => ["date", v.toISOString()],
  onBuffer: (v) => ["buffer", v.toString("base64")],
  onArray: (children) => ["array", children],
  onObject: (entries) => ["object", entries],
};

function canonicalize(value) {
  return walk(value, CANONICAL_HANDLERS);
}

function encodeNode(value) {
  return walk(value, ENCODED_HANDLERS);
}

// Encode in a single walk while tracking whether any extended type (Date or
// Buffer) was seen, so callers can decide on the dedup representation without a
// second `containsExtendedType` traversal.
function encodeNodeTracked(value) {
  const state = { extended: false };
  const handlers = {
    ...ENCODED_HANDLERS,
    onDate: (v) => {
      state.extended = true;
      return ENCODED_HANDLERS.onDate(v);
    },
    onBuffer: (v) => {
      state.extended = true;
      return ENCODED_HANDLERS.onBuffer(v);
    },
  };
  return { value: walk(value, handlers), extended: state.extended };
}

function decodeNode(node) {
  const [type, value] = node;
  switch (type) {
    case "null": return null;
    case "string": case "boolean": case "number": return value;
    case "date": return new Date(value);
    case "buffer": return Buffer.from(value, "base64");
    case "array": return value.map(decodeNode);
    case "object": return Object.fromEntries(value.map(([key, item]) => [key, decodeNode(item)]));
    default: {
      const error = new Error(`Unknown outbox payload type: ${type}`);
      error.code = "OUTBOX_INVALID_PAYLOAD";
      throw error;
    }
  }
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

function stableStringify(value) {
  const canonical = canonicalize(value);
  if (canonical === undefined) {
    throw new TypeError("The outbox payload cannot be undefined");
  }
  if (containsExtendedType(value)) {
    return `canonical-v2:${encodePayload(value)}`;
  }
  return JSON.stringify(canonical);
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

/**
 * Produce both persisted forms of a payload in one place:
 *   - `payloadJson`: the canonical-v2 storage string (identical to
 *     `encodePayload(value)`).
 *   - `dedupeString`: the stable string hashed into the dedup key (identical to
 *     `stableStringify(value)`).
 * Extended-type detection is folded into the encode walk, so a payload with no
 * Date/Buffer costs two walks (encode + canonicalize) and one with extended
 * types costs a single walk, versus up to four across the separate helpers.
 * @param {any} value
 * @returns {{payloadJson: string, dedupeString: string}}
 */
function serializePayload(value) {
  if (value === undefined) {
    throw new TypeError("The outbox payload cannot be undefined");
  }
  const { value: encoded, extended } = encodeNodeTracked(value);
  const payloadJson = JSON.stringify({
    format: "node-red-outbox",
    version: 2,
    value: encoded,
  });
  // Mirrors `stableStringify`: for extended-type payloads the storage string is
  // already canonical (sorted keys), so it doubles as the dedup string.
  const dedupeString = extended
    ? `canonical-v2:${payloadJson}`
    : JSON.stringify(canonicalize(value));
  return { payloadJson, dedupeString };
}

function decodePayload(json, encoding = "json-v1") {
  if (encoding !== "json-v1" && encoding !== "canonical-v2") {
    const error = new Error(`Unsupported outbox payload encoding: ${encoding}`);
    error.code = "OUTBOX_UNSUPPORTED_ENCODING";
    throw error;
  }
  let parsed;
  try {
    parsed = JSON.parse(json);
  } catch (cause) {
    const error = new Error(`Invalid outbox payload JSON: ${cause.message}`);
    error.code = "OUTBOX_INVALID_PAYLOAD";
    error.cause = cause;
    throw error;
  }
  if (encoding === "canonical-v2") {
    if (parsed?.format !== "node-red-outbox" || parsed.version !== 2) {
      const error = new Error("Invalid canonical-v2 outbox payload");
      error.code = "OUTBOX_INVALID_PAYLOAD";
      throw error;
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

module.exports = {
  canonicalize,
  encodeNode,
  decodeNode,
  containsExtendedType,
  stableStringify,
  encodePayload,
  serializePayload,
  decodePayload,
  serializeError,
};
