"use strict";

function megabytesToBytes(value, fallbackMb) {
  const megabytes = Number(value);
  const normalized =
    Number.isFinite(megabytes) && megabytes > 0 ? megabytes : fallbackMb;
  return Math.max(1, Math.round(normalized * 1_048_576));
}

function nonnegativeMegabytesToBytes(value, fallbackMb) {
  const megabytes = Number(value);
  const normalized =
    Number.isFinite(megabytes) && megabytes >= 0 ? megabytes : fallbackMb;
  return Math.max(0, Math.round(normalized * 1_048_576));
}

function parseDuration(value, defaultMs) {
  const fallback = Math.max(0, Number(defaultMs) || 0);
  if (value == null || value === "") return fallback;
  if (typeof value === "number") return Number.isFinite(value) ? Math.max(0, value) : fallback;
  if (typeof value !== "string") return fallback;

  const trimmed = value.trim();
  if (trimmed === "" || trimmed === "0") return 0;

  const numberMatch = /^\d+/.exec(trimmed);
  if (!numberMatch) return fallback;
  const quantity = Number(numberMatch[0]);
  if (!Number.isFinite(quantity)) return fallback;

  const suffix = trimmed.slice(numberMatch[0].length).trimStart();
  let multiplier;
  switch (suffix.toLowerCase()) {
    case "d": multiplier = 86_400_000; break;
    case "h": multiplier = 3_600_000; break;
    case "m": multiplier = 60_000; break;
    case "s": multiplier = 1_000; break;
    case "ms": multiplier = 1; break;
    case "": multiplier = 1; break;   // bare number = ms passthrough
    default: return fallback;
  }
  return Math.max(0, quantity * multiplier);
}

module.exports = {
  megabytesToBytes,
  nonnegativeMegabytesToBytes,
  parseDuration,
};
