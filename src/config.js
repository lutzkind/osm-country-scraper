const path = require("path");

function intFromEnv(name, fallback) {
  const value = process.env[name];
  if (value == null || value === "") {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function floatFromEnv(name, fallback) {
  const value = process.env[name];
  if (value == null || value === "") {
    return fallback;
  }

  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function boolFromEnv(name, fallback) {
  const value = process.env[name];
  if (value == null || value === "") {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
}

function arrayFromEnv(name, fallback = []) {
  const value = process.env[name];
  if (value == null || value === "") {
    return fallback;
  }

  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

const dataDir = process.env.DATA_DIR || path.join(process.cwd(), "data");

module.exports = {
  host: process.env.HOST || "0.0.0.0",
  port: intFromEnv("PORT", 3000),
  dataDir,
  dbPath: process.env.DB_PATH || path.join(dataDir, "osm-country-scraper.db"),
  exportsDir: process.env.EXPORTS_DIR || path.join(dataDir, "exports"),
  publicBaseUrl: process.env.PUBLIC_BASE_URL || null,
  userAgent:
    process.env.USER_AGENT ||
    "osm-country-scraper/1.0 (+mailto:lutz.kind96@gmail.com)",
  nominatimUrl:
    process.env.NOMINATIM_URL ||
    "https://nominatim.openstreetmap.org/search",
  overpassUrl:
    process.env.OVERPASS_URL || "https://overpass-api.de/api/interpreter",
  workerPollMs: intFromEnv("WORKER_POLL_MS", 5000),
  overpassDelayMs: intFromEnv("OVERPASS_DELAY_MS", 2000),
  overpassTimeoutMs: intFromEnv("OVERPASS_TIMEOUT_MS", 120000),
  overpassQueryTimeoutSec: intFromEnv("OVERPASS_QUERY_TIMEOUT_SEC", 90),
  maxShardDepth: intFromEnv("MAX_SHARD_DEPTH", 8),
  retryLimit: intFromEnv("RETRY_LIMIT", 6),
  retryBaseDelayMs: intFromEnv("RETRY_BASE_DELAY_MS", 60000),
  resultSplitThreshold: intFromEnv("RESULT_SPLIT_THRESHOLD", 250),
  minShardWidthDeg: floatFromEnv("MIN_SHARD_WIDTH_DEG", 0.1),
  minShardHeightDeg: floatFromEnv("MIN_SHARD_HEIGHT_DEG", 0.1),
  adminUsername: process.env.ADMIN_USERNAME || null,
  adminPassword: process.env.ADMIN_PASSWORD || null,
  sessionCookieName: process.env.SESSION_COOKIE_NAME || "osm_scraper_session",
  sessionTtlHours: intFromEnv("SESSION_TTL_HOURS", 24),
  nocoDb: {
    baseUrl: process.env.NOCODB_BASE_URL || null,
    apiToken: process.env.NOCODB_API_TOKEN || null,
    baseId: process.env.NOCODB_BASE_ID || null,
    tableId: process.env.NOCODB_TABLE_ID || null,
    autoSyncOnCompletion: boolFromEnv("NOCODB_AUTO_SYNC_ON_COMPLETION", false),
    autoSyncIntervalMinutes: intFromEnv("NOCODB_AUTO_SYNC_INTERVAL_MINUTES", 0),
    autoCreateColumns: boolFromEnv("NOCODB_AUTO_CREATE_COLUMNS", true),
    promotedTags: arrayFromEnv("NOCODB_PROMOTED_TAGS", []),
  },
};
