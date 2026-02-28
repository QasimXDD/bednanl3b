const http = require("http");
const fs = require("fs");
const https = require("https");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
const { Readable } = require("stream");
const { pipeline: streamPipeline } = require("stream/promises");
const { WebSocketServer } = require("ws");
const { Client: PgClient } = require("pg");
const mysql = require("mysql2/promise");
require("dotenv").config();
if (!process.env.YTDL_NO_DEBUG_FILE) {
  process.env.YTDL_NO_DEBUG_FILE = "1";
}
if (!process.env.YTDL_NO_UPDATE) {
  process.env.YTDL_NO_UPDATE = "1";
}
const ytdl = require("@distube/ytdl-core");
let ybdYtdlModule = null;
try {
  ybdYtdlModule = require("@ybd-project/ytdl-core");
} catch (_error) {
  ybdYtdlModule = null;
}
const Busboy = require("busboy");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const RENDER_DATA_ROOT = path.resolve(
  String(process.env.RENDER_DISK_ROOT || path.join(os.tmpdir(), "sawawatch"))
);
const DEFAULT_DATA_DIR = process.env.RENDER
  ? path.join(RENDER_DATA_ROOT, "data")
  : path.join(ROOT, "data");
const DEFAULT_UPLOADS_DIR = process.env.RENDER
  ? path.join(RENDER_DATA_ROOT, "uploads")
  : path.join(ROOT, "uploads");
const DATA_DIR = path.resolve(String(process.env.DATA_DIR || DEFAULT_DATA_DIR));
const UPLOADS_DIR = path.resolve(String(process.env.UPLOADS_DIR || DEFAULT_UPLOADS_DIR));
const ROOM_VIDEO_PUBLIC_PREFIX = "/uploads/room-videos/";
const ROOM_VIDEO_BLOB_API_PREFIX = "/api/video-blob/";
const ROOM_VIDEO_UPLOAD_DIR_DEFAULT = path.join(UPLOADS_DIR, "room-videos");
const ROOM_VIDEO_UPLOAD_DIR_FALLBACK = path.join(os.tmpdir(), "bednanl3b-room-videos");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const USERS_BACKUP_FILE = path.join(DATA_DIR, "users.backup.json");
const MODERATION_FILE = path.join(DATA_DIR, "moderation.json");
const YOUTUBE_CACHE_FILE = path.join(DATA_DIR, "youtube-cache.json");
const AUDIT_LOG_FILE = path.join(DATA_DIR, "audit-log.jsonl");
const SUPERVISOR_USERNAME = "qasim";
const GUEST_USERNAME_PREFIX = "guest_";
const SITE_ANNOUNCEMENT_LIVE_MS = 15000;
const SESSION_TTL_MS = 1000 * 60 * 60 * 24 * 7;
const USERNAME_MIN_LENGTH = 3;
const USERNAME_MAX_LENGTH = 30;
const PASSWORD_MIN_LENGTH = 4;
const PASSWORD_MAX_LENGTH = 128;
const ROOM_NAME_MAX_LENGTH = 40;
const ROOM_EMPTY_DELETE_DELAY_MS = 10000;
const USERNAME_ALLOWED_REGEX = /^[\p{L}\p{N}_-]+$/u;
const REGISTERED_USERNAME_ALLOWED_REGEX = /^[a-z0-9]+$/;
const COUNTRY_CODE_REGEX = /^[A-Z]{2}$/;
const CLIENT_MESSAGE_ID_MAX_LENGTH = 80;
const CLIENT_MESSAGE_ID_REGEX = /^[A-Za-z0-9_-]+$/;
const MESSAGE_DEDUP_TTL_MS = 1000 * 60 * 5;
const REPLY_PREVIEW_MAX_LENGTH = 160;
const CHAT_REACTION_ALLOWED = new Set(["👍", "❤️", "😂", "🔥", "😮", "😢"]);
const CHAT_REACTION_MAX_TYPES_PER_MESSAGE = 6;
const ROOM_VIDEO_MAX_BYTES = 1024 * 1024 * 1024;
const ROOM_VIDEO_MULTIPART_OVERHEAD_BYTES = 8 * 1024 * 1024;
const ROOM_VIDEO_MAX_DURATION_SEC = 60 * 60 * 8;
const ROOM_VIDEO_MAX_FILENAME_LENGTH = 120;
const ROOM_VIDEO_HEARTBEAT_REWIND_GUARD_SEC = 2.4;
const ROOM_VIDEO_DB_STREAM_CHUNK_BYTES = 1024 * 1024;
const YOUTUBE_SEARCH_TIMEOUT_MS = 9000;
const YOUTUBE_DOWNLOAD_TIMEOUT_MS = 1000 * 60 * 8;
const YOUTUBE_INPUT_MAX_LENGTH = 300;
const YOUTUBE_VIDEO_ID_REGEX = /^[A-Za-z0-9_-]{11}$/;
const YOUTUBE_CACHE_MAX_ITEMS = 180;
const YOUTUBE_CACHE_MAX_AGE_MS = 1000 * 60 * 60 * 24 * 30;
const YOUTUBE_PROXY_TTL_MS = 1000 * 60 * 60 * 6;
const YOUTUBE_OUTBOUND_PROXY = String(
  process.env.YOUTUBE_OUTBOUND_PROXY || process.env.HTTPS_PROXY || process.env.HTTP_PROXY || ""
).trim();
const YT_DLP_BINARY = String(process.env.YT_DLP_BINARY || "yt-dlp").trim() || "yt-dlp";
const YT_DLP_EMBEDDED_BINARY = (() => {
  try {
    const youtubeDlExec = require("youtube-dl-exec");
    return String(youtubeDlExec?.constants?.YOUTUBE_DL_PATH || "").trim();
  } catch (_error) {
    return "";
  }
})();
const YT_DLP_AUTO_DOWNLOAD_URL = String(
  process.env.YT_DLP_AUTO_DOWNLOAD_URL || "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp"
).trim();
const YT_DLP_AUTO_DOWNLOAD_URLS = Array.from(
  new Set(
    [
      YT_DLP_AUTO_DOWNLOAD_URL,
      "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp",
      "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux"
    ]
      .map((url) => String(url || "").trim())
      .filter(Boolean)
  )
);
const YT_DLP_AUTO_DOWNLOAD_TIMEOUT_MS = Math.max(
  15000,
  Number(process.env.YT_DLP_AUTO_DOWNLOAD_TIMEOUT_MS || 1000 * 60 * 2) || 1000 * 60 * 2
);
const YT_DLP_AUTO_BINARY_PATH = path.join(
  DATA_DIR,
  "bin",
  process.platform === "win32" ? "yt-dlp.exe" : "yt-dlp"
);
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const MYSQL_HOST = String(process.env.MYSQL_HOST || "").trim();
const MYSQL_PORT = Math.max(1, Number(process.env.MYSQL_PORT || 3306) || 3306);
const MYSQL_DATABASE = String(process.env.MYSQL_DATABASE || "").trim();
const MYSQL_USER = String(process.env.MYSQL_USER || "").trim();
const MYSQL_PASSWORD = String(process.env.MYSQL_PASSWORD || "");
const MYSQL_SSL = String(process.env.MYSQL_SSL || "").trim().toLowerCase();
const PG_RUNTIME_SNAPSHOT_DEBOUNCE_MS = 1200;
const PG_RUNTIME_PERSIST_MAX_MESSAGES_PER_ROOM = 500;
const WEBSOCKET_PATH = "/ws";
const WS_PING_INTERVAL_MS = 30000;
const WS_RETAINED_EVENT_WINDOW_MS = 15000;
const GITHUB_USERS_SYNC_TOKEN = String(process.env.GITHUB_USERS_SYNC_TOKEN || "").trim();
const GITHUB_USERS_SYNC_REPO = String(process.env.GITHUB_USERS_SYNC_REPO || "").trim();
const GITHUB_USERS_SYNC_BRANCH = String(process.env.GITHUB_USERS_SYNC_BRANCH || "main").trim() || "main";
const GITHUB_USERS_SYNC_PATH = String(process.env.GITHUB_USERS_SYNC_PATH || "data/users.json")
  .replace(/\\/g, "/")
  .replace(/^\/+/, "")
  .trim() || "data/users.json";
const GITHUB_USERS_SYNC_TIMEOUT_MS = 15000;
const ROOM_VIDEO_ALLOWED_MIME_TYPES = new Set([
  "video/mp4",
  "video/webm",
  "video/ogg"
]);
const NON_ISO_COUNTRY_CODES = new Set(["XX", "ZZ", "A1", "A2", "O1", "T1", "EU", "AP"]);
const BLOCKED_STATIC_FILES = new Set([
  "server.js",
  "package.json",
  "package-lock.json",
  "yarn.lock",
  "pnpm-lock.yaml",
  "npm-shrinkwrap.json",
  "agents.md"
]);
const BLOCKED_STATIC_DIRS = new Set(["data", ".git", "node_modules"]);
const SITE_PRESENCE_AUDIT_ACTION_LIST = Object.freeze([
  "site_presence_enter",
  "site_presence_exit",
  "auth_register_success",
  "auth_login_success",
  "auth_guest_login"
]);
const SITE_PRESENCE_AUDIT_ACTIONS = new Set(SITE_PRESENCE_AUDIT_ACTION_LIST);
const SITE_PRESENCE_LOG_DEFAULT_LIMIT = 120;
const SITE_PRESENCE_LOG_MAX_LIMIT = 500;
const RATE_LIMIT_MESSAGE = "Too many requests. Please wait and try again.";
const RATE_LIMITS = Object.freeze({
  REGISTER: { key: "register", max: 12, windowMs: 10 * 60 * 1000 },
  LOGIN: { key: "login", max: 30, windowMs: 10 * 60 * 1000 },
  GUEST_LOGIN: { key: "guest-login", max: 25, windowMs: 10 * 60 * 1000 },
  BAN_APPEAL: { key: "ban-appeal", max: 8, windowMs: 10 * 60 * 1000 },
  ROOMS_CREATE: { key: "rooms-create", max: 20, windowMs: 10 * 60 * 1000 },
  ROOMS_JOIN: { key: "rooms-join", max: 60, windowMs: 10 * 60 * 1000 },
  ROOMS_REQUEST_JOIN: { key: "rooms-request-join", max: 70, windowMs: 10 * 60 * 1000 },
  ROOM_MESSAGE_POST: { key: "room-message-post", max: 180, windowMs: 10 * 60 * 1000 },
  ROOM_VIDEO_UPLOAD: { key: "room-video-upload", max: 10, windowMs: 10 * 60 * 1000 },
  ROOM_VIDEO_SOURCE: { key: "room-video-source", max: 30, windowMs: 10 * 60 * 1000 },
  ADMIN_ACTION: { key: "admin-action", max: 120, windowMs: 10 * 60 * 1000 }
});

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".mp4": "video/mp4",
  ".webm": "video/webm",
  ".ogg": "video/ogg",
  ".mov": "video/quicktime",
  ".ico": "image/x-icon"
};

const users = new Map();
const guestUsers = new Map();
const sessions = new Map();
const rooms = new Map();
const roomEmptyDeletionTimers = new Map();
const userLastSeen = new Map();
const bannedUsers = new Map();
const unbanRequests = new Map();
const youtubeVideoCache = new Map();
const youtubeProxyStreams = new Map();
const roomSocketSubscribers = new Map();
const wsClientMetadata = new WeakMap();
const rateLimitBuckets = new Map();
let siteAnnouncement = null;
let activeRoomVideoUploadDir = ROOM_VIDEO_UPLOAD_DIR_DEFAULT;
let ytDlpCommandOverride = null;
let ytDlpAutoInstallPromise = null;
let ybdYtdlClient = null;
let gitHubUsersSyncQueue = Promise.resolve();
let gitHubUsersFileSha = "";
let wsServer = null;
let wsPingTimer = null;
let pgClient = null;
let pgRuntimeEnabled = false;
let mysqlPool = null;
let mysqlRuntimeEnabled = false;
let pgRuntimePersistTimer = null;
let pgRuntimePersistInFlight = false;
let pgRuntimePersistQueued = false;
let usersSqlPersistTimer = null;
let usersSqlPersistInFlight = false;
let usersSqlPersistQueued = false;

function parseJsonWithOptionalBom(raw) {
  return JSON.parse(String(raw || "").replace(/^\uFEFF/, ""));
}

function getClientIp(req) {
  const forwarded = String(req?.headers?.["x-forwarded-for"] || "");
  const forwardedPart = forwarded.split(",")[0]?.trim();
  const socketIp = String(req?.socket?.remoteAddress || "").trim();
  const raw = forwardedPart || socketIp || "unknown";
  return raw.replace(/^::ffff:/i, "");
}

function getRateLimitBucketKey(req, key, scope = "") {
  const ip = getClientIp(req);
  return `${String(key || "global")}::${ip}::${String(scope || "")}`;
}

function enforceRateLimit(req, res, rule, scope = "") {
  if (!rule || !Number.isFinite(rule.max) || !Number.isFinite(rule.windowMs)) {
    return true;
  }
  const bucketKey = getRateLimitBucketKey(req, rule.key, scope);
  const now = Date.now();
  const windowMs = Math.max(1000, Number(rule.windowMs));
  const maxHits = Math.max(1, Number(rule.max));
  const bucket = rateLimitBuckets.get(bucketKey);
  if (!bucket || now >= bucket.resetAt) {
    rateLimitBuckets.set(bucketKey, {
      count: 1,
      resetAt: now + windowMs
    });
    return true;
  }
  if (bucket.count >= maxHits) {
    const retryAfterMs = Math.max(0, bucket.resetAt - now);
    const retryAfterSec = Math.max(1, Math.ceil(retryAfterMs / 1000));
    sendJson(res, 429, {
      code: "RATE_LIMITED",
      error: i18n(req, RATE_LIMIT_MESSAGE, RATE_LIMIT_MESSAGE),
      retryAfterSec
    });
    return false;
  }
  bucket.count += 1;
  rateLimitBuckets.set(bucketKey, bucket);
  return true;
}

function pruneRateLimitBuckets(now = Date.now()) {
  rateLimitBuckets.forEach((bucket, key) => {
    if (!bucket || Number(bucket.resetAt || 0) <= now) {
      rateLimitBuckets.delete(key);
    }
  });
}

function sanitizeAuditValue(value, maxLength = 400) {
  return String(value || "").trim().slice(0, maxLength);
}

function writeAuditEntryToFile(entry) {
  try {
    ensureDataDir();
    fs.appendFileSync(AUDIT_LOG_FILE, `${JSON.stringify(entry)}\n`, "utf8");
  } catch (error) {
    console.error("Failed to write audit entry:", error.message);
  }
}

function recordAudit(action, actor, target = "", details = {}) {
  const entry = {
    id: `${Date.now()}-${crypto.randomBytes(5).toString("hex")}`,
    createdAt: Date.now(),
    action: sanitizeAuditValue(action, 120) || "unknown_action",
    actor: sanitizeAuditValue(actor, 120) || "system",
    target: sanitizeAuditValue(target, 180),
    details: details && typeof details === "object" ? details : {}
  };
  writeAuditEntryToFile(entry);
  if (pgRuntimeEnabled && pgClient) {
    pgClient.query(
      "INSERT INTO app_audit_log (created_at, action, actor, target, details) VALUES ($1, $2, $3, $4, $5::jsonb)",
      [
        Number(entry.createdAt),
        entry.action,
        entry.actor,
        entry.target,
        JSON.stringify(entry.details || {})
      ]
    ).catch((error) => {
      console.error("Failed to persist audit entry:", error.message);
    });
    return;
  }
  if (!mysqlRuntimeEnabled || !mysqlPool) {
    return;
  }
  mysqlPool.execute(
    "INSERT INTO app_audit_log (created_at, action, actor, target, details) VALUES (?, ?, ?, ?, ?)",
    [
      Number(entry.createdAt),
      entry.action,
      entry.actor,
      entry.target,
      JSON.stringify(entry.details || {})
    ]
  ).catch((error) => {
    console.error("Failed to persist audit entry:", error.message);
  });
}

function clampSitePresenceLimit(rawLimit) {
  const parsed = Number(rawLimit);
  if (!Number.isFinite(parsed)) {
    return SITE_PRESENCE_LOG_DEFAULT_LIMIT;
  }
  return Math.max(1, Math.min(SITE_PRESENCE_LOG_MAX_LIMIT, Math.floor(parsed)));
}

function getSitePresenceEventTypeFromAction(action) {
  const cleanAction = String(action || "").trim();
  if (cleanAction === "site_presence_exit") {
    return "exit";
  }
  if (SITE_PRESENCE_AUDIT_ACTIONS.has(cleanAction)) {
    return "enter";
  }
  return "";
}

function getSitePresenceSourceFromAction(action, details = {}) {
  const cleanAction = String(action || "").trim();
  const sourceFromDetails = sanitizeAuditValue(details?.source, 40).toLowerCase();
  if (sourceFromDetails) {
    return sourceFromDetails;
  }
  if (cleanAction === "auth_register_success") {
    return "register";
  }
  if (cleanAction === "auth_login_success") {
    return "login";
  }
  if (cleanAction === "auth_guest_login") {
    return "guest";
  }
  if (cleanAction === "site_presence_exit") {
    return "logout";
  }
  return "login";
}

function normalizeSitePresenceEvent(row) {
  if (!row || typeof row !== "object") {
    return null;
  }
  const action = sanitizeAuditValue(row.action, 120);
  const eventType = getSitePresenceEventTypeFromAction(action);
  if (!eventType) {
    return null;
  }
  const details = decodeSqlJson(row.details, {});
  const username = normalizeUsername(
    row.actor || row.target || details?.username || details?.user || details?.displayName || ""
  );
  const createdAt = Number(row.createdAt || row.created_at || Date.now()) || Date.now();
  const id = sanitizeAuditValue(
    row.id || `${createdAt}-${action}-${username || "unknown"}`,
    120
  ) || `${createdAt}-${action}`;
  const countryCode = normalizeCountryCode(details?.countryCode || details?.country || "");
  const ip = sanitizeAuditValue(details?.ip || details?.clientIp || "", 120);
  const displayName = sanitizeAuditValue(details?.displayName || "", 120);
  const isGuest = Boolean(details?.isGuest) || action === "auth_guest_login";
  return {
    id,
    createdAt,
    eventType,
    source: getSitePresenceSourceFromAction(action, details),
    username: username || "unknown",
    displayName,
    countryCode,
    ip,
    isGuest
  };
}

function readSitePresenceEventsFromAuditFile(limit) {
  if (!fs.existsSync(AUDIT_LOG_FILE)) {
    return [];
  }
  try {
    const raw = fs.readFileSync(AUDIT_LOG_FILE, "utf8");
    if (!raw.trim()) {
      return [];
    }
    const lines = raw.split(/\r?\n/);
    const list = [];
    for (let index = lines.length - 1; index >= 0; index -= 1) {
      const line = String(lines[index] || "").trim();
      if (!line) {
        continue;
      }
      let parsed = null;
      try {
        parsed = JSON.parse(line);
      } catch (_error) {
        parsed = null;
      }
      if (!parsed || !SITE_PRESENCE_AUDIT_ACTIONS.has(String(parsed.action || "").trim())) {
        continue;
      }
      const normalized = normalizeSitePresenceEvent({
        id: parsed.id,
        createdAt: parsed.createdAt,
        action: parsed.action,
        actor: parsed.actor,
        target: parsed.target,
        details: parsed.details
      });
      if (!normalized) {
        continue;
      }
      list.push(normalized);
      if (list.length >= limit) {
        break;
      }
    }
    return list;
  } catch (error) {
    console.error("Failed to read site presence from audit file:", error.message);
    return [];
  }
}

async function listSitePresenceEvents(limit = SITE_PRESENCE_LOG_DEFAULT_LIMIT) {
  const safeLimit = clampSitePresenceLimit(limit);
  if (pgRuntimeEnabled && pgClient) {
    try {
      const result = await pgClient.query(
        `SELECT id::text AS id, created_at AS "createdAt", action, actor, target, details
         FROM app_audit_log
         WHERE action = ANY($1::text[])
         ORDER BY created_at DESC
         LIMIT $2`,
        [SITE_PRESENCE_AUDIT_ACTION_LIST, safeLimit]
      );
      return result.rows
        .map((row) => normalizeSitePresenceEvent(row))
        .filter(Boolean);
    } catch (error) {
      console.error("Failed to load site presence from PostgreSQL:", error.message);
    }
  }
  if (mysqlRuntimeEnabled && mysqlPool) {
    try {
      const placeholders = SITE_PRESENCE_AUDIT_ACTION_LIST.map(() => "?").join(", ");
      const [rows] = await mysqlPool.query(
        `SELECT CAST(id AS CHAR) AS id, created_at AS createdAt, action, actor, target, details
         FROM app_audit_log
         WHERE action IN (${placeholders})
         ORDER BY created_at DESC
         LIMIT ?`,
        [...SITE_PRESENCE_AUDIT_ACTION_LIST, safeLimit]
      );
      return (Array.isArray(rows) ? rows : [])
        .map((row) => normalizeSitePresenceEvent(row))
        .filter(Boolean);
    } catch (error) {
      console.error("Failed to load site presence from MySQL:", error.message);
    }
  }
  return readSitePresenceEventsFromAuditFile(safeLimit);
}

function normalizeRoomVideoCollections(room) {
  if (!room) {
    return;
  }
  if (!Array.isArray(room.videoQueue)) {
    room.videoQueue = [];
  }
  if (!Array.isArray(room.videoHistory)) {
    room.videoHistory = [];
  }
}

function getRoomSubscriberSet(roomCode, create = false) {
  const code = String(roomCode || "").trim().toUpperCase();
  if (!code) {
    return null;
  }
  const existing = roomSocketSubscribers.get(code);
  if (existing) {
    return existing;
  }
  if (!create) {
    return null;
  }
  const set = new Set();
  roomSocketSubscribers.set(code, set);
  return set;
}

function sendWsJson(ws, payload) {
  if (!ws || ws.readyState !== ws.OPEN) {
    return;
  }
  try {
    ws.send(JSON.stringify(payload));
  } catch (_error) {
    // Ignore transport errors; close handlers will clean up.
  }
}

function removeWsSubscriber(ws) {
  const meta = wsClientMetadata.get(ws);
  if (!meta) {
    return;
  }
  const subscribers = getRoomSubscriberSet(meta.roomCode, false);
  if (subscribers) {
    subscribers.delete(ws);
    if (subscribers.size === 0) {
      roomSocketSubscribers.delete(meta.roomCode);
    }
  }
  wsClientMetadata.delete(ws);
}

function disconnectRoomMemberSockets(roomCode, username, reason = "membership_changed") {
  const code = String(roomCode || "").trim().toUpperCase();
  const user = normalizeUsername(username);
  if (!code || !user) {
    return;
  }
  const subscribers = getRoomSubscriberSet(code, false);
  if (!subscribers || subscribers.size === 0) {
    return;
  }
  Array.from(subscribers).forEach((ws) => {
    const meta = wsClientMetadata.get(ws);
    if (!meta || meta.username !== user) {
      return;
    }
    sendWsJson(ws, {
      type: "room_forbidden",
      roomCode: code,
      reason
    });
    try {
      ws.close(4003, "room-membership-updated");
    } catch (_error) {
      // Ignore close errors.
    }
  });
}

function broadcastRoomEvent(roomCode, type, payload = {}) {
  const code = String(roomCode || "").trim().toUpperCase();
  if (!code || !type) {
    return;
  }
  const subscribers = getRoomSubscriberSet(code, false);
  if (!subscribers || subscribers.size === 0) {
    return;
  }
  const packet = {
    type: String(type),
    roomCode: code,
    timestamp: Date.now(),
    ...payload
  };
  Array.from(subscribers).forEach((ws) => {
    if (!ws || ws.readyState !== ws.OPEN) {
      removeWsSubscriber(ws);
      return;
    }
    sendWsJson(ws, packet);
  });
}

function broadcastRoomTouch(room, reason, payload = {}) {
  if (!room || !room.code) {
    return;
  }
  broadcastRoomEvent(room.code, "room_changed", {
    reason: String(reason || "updated"),
    ...payload
  });
}

function broadcastVideoSync(room) {
  if (!room || !room.code) {
    return;
  }
  broadcastRoomEvent(room.code, "video_sync", {
    video: formatRoomVideo(room),
    queue: formatRoomVideoQueue(room),
    historyCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
  });
}

function hasMySqlRuntimeConfig() {
  return Boolean(MYSQL_HOST && MYSQL_DATABASE && MYSQL_USER);
}

function getMySqlHostCandidates() {
  const hosts = new Set();
  if (MYSQL_HOST) {
    hosts.add(MYSQL_HOST);
    // Common typo in FreeSQLHosting docs/screenshots.
    if (MYSQL_HOST.endsWith(".freesqldhosting.net")) {
      hosts.add(MYSQL_HOST.replace(".freesqldhosting.net", ".freesqldatabase.com"));
    }
  }
  return Array.from(hosts).filter(Boolean);
}

function isSqlRuntimeEnabled() {
  return Boolean(pgRuntimeEnabled || mysqlRuntimeEnabled);
}

function decodeSqlJson(value, fallback = null) {
  if (value === null || value === undefined) {
    return fallback;
  }
  if (typeof value === "object") {
    return value;
  }
  const raw = String(value || "").trim();
  if (!raw) {
    return fallback;
  }
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return fallback;
  }
}

async function ensurePgRuntimeSchema() {
  if (!pgClient) {
    return;
  }
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_runtime_sessions (
      token TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      expires_at BIGINT NOT NULL,
      is_guest BOOLEAN NOT NULL DEFAULT FALSE
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_runtime_rooms (
      code TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      host TEXT NOT NULL,
      pending_host_restore TEXT,
      created_at BIGINT NOT NULL,
      next_message_id BIGINT NOT NULL,
      video JSONB,
      video_sync JSONB,
      video_queue JSONB,
      video_history JSONB
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_runtime_room_members (
      room_code TEXT NOT NULL,
      username TEXT NOT NULL,
      approved BOOLEAN NOT NULL DEFAULT TRUE,
      PRIMARY KEY (room_code, username)
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_runtime_room_join_requests (
      room_code TEXT NOT NULL,
      username TEXT NOT NULL,
      PRIMARY KEY (room_code, username)
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_runtime_room_messages (
      room_code TEXT NOT NULL,
      message_id BIGINT NOT NULL,
      payload JSONB NOT NULL,
      PRIMARY KEY (room_code, message_id)
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_video_assets (
      id TEXT PRIMARY KEY,
      mime_type TEXT NOT NULL,
      byte_size BIGINT NOT NULL,
      payload BYTEA NOT NULL,
      created_at BIGINT NOT NULL,
      source_kind TEXT NOT NULL DEFAULT 'upload',
      file_name TEXT NOT NULL DEFAULT '',
      youtube_id TEXT NOT NULL DEFAULT ''
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_users (
      username TEXT PRIMARY KEY,
      password_hash TEXT NOT NULL,
      password_salt TEXT NOT NULL DEFAULT '',
      created_at BIGINT NOT NULL,
      display_name TEXT NOT NULL,
      avatar_data_url TEXT NOT NULL DEFAULT '',
      country_code TEXT NOT NULL DEFAULT ''
    );
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS app_audit_log (
      id BIGSERIAL PRIMARY KEY,
      created_at BIGINT NOT NULL,
      action TEXT NOT NULL,
      actor TEXT NOT NULL,
      target TEXT NOT NULL,
      details JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);
}

async function initPgRuntime() {
  if (!DATABASE_URL) {
    return;
  }
  try {
    pgClient = new PgClient({
      connectionString: DATABASE_URL
    });
    await pgClient.connect();
    await ensurePgRuntimeSchema();
    pgRuntimeEnabled = true;
    console.log("PostgreSQL runtime persistence is enabled.");
  } catch (error) {
    pgRuntimeEnabled = false;
    pgClient = null;
    console.error("Failed to initialize PostgreSQL runtime persistence:", error.message);
  }
}

async function ensureMySqlRuntimeSchema() {
  if (!mysqlPool) {
    return;
  }
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_runtime_sessions (
      token VARCHAR(255) PRIMARY KEY,
      username VARCHAR(120) NOT NULL,
      expires_at BIGINT NOT NULL,
      is_guest TINYINT(1) NOT NULL DEFAULT 0
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_runtime_rooms (
      code VARCHAR(32) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      host VARCHAR(120) NOT NULL,
      pending_host_restore VARCHAR(120) NULL,
      created_at BIGINT NOT NULL,
      next_message_id BIGINT NOT NULL,
      video LONGTEXT NULL,
      video_sync LONGTEXT NULL,
      video_queue LONGTEXT NULL,
      video_history LONGTEXT NULL
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_runtime_room_members (
      room_code VARCHAR(32) NOT NULL,
      username VARCHAR(120) NOT NULL,
      approved TINYINT(1) NOT NULL DEFAULT 1,
      PRIMARY KEY (room_code, username)
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_runtime_room_join_requests (
      room_code VARCHAR(32) NOT NULL,
      username VARCHAR(120) NOT NULL,
      PRIMARY KEY (room_code, username)
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_runtime_room_messages (
      room_code VARCHAR(32) NOT NULL,
      message_id BIGINT NOT NULL,
      payload LONGTEXT NOT NULL,
      PRIMARY KEY (room_code, message_id)
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_video_assets (
      id VARCHAR(80) PRIMARY KEY,
      mime_type VARCHAR(160) NOT NULL,
      byte_size BIGINT NOT NULL,
      payload LONGBLOB NOT NULL,
      created_at BIGINT NOT NULL,
      source_kind VARCHAR(32) NOT NULL DEFAULT 'upload',
      file_name VARCHAR(255) NOT NULL DEFAULT '',
      youtube_id VARCHAR(40) NOT NULL DEFAULT ''
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_users (
      username VARCHAR(120) PRIMARY KEY,
      password_hash LONGTEXT NOT NULL,
      password_salt VARCHAR(255) NOT NULL DEFAULT '',
      created_at BIGINT NOT NULL,
      display_name VARCHAR(255) NOT NULL,
      avatar_data_url LONGTEXT NOT NULL,
      country_code VARCHAR(16) NOT NULL DEFAULT ''
    )
  `);
  await mysqlPool.execute(`
    CREATE TABLE IF NOT EXISTS app_audit_log (
      id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      created_at BIGINT NOT NULL,
      action VARCHAR(180) NOT NULL,
      actor VARCHAR(180) NOT NULL,
      target VARCHAR(255) NOT NULL,
      details LONGTEXT NOT NULL
    )
  `);
}

async function initMySqlRuntime() {
  if (!hasMySqlRuntimeConfig()) {
    return;
  }
  const hostCandidates = getMySqlHostCandidates();
  let lastError = null;
  for (const hostCandidate of hostCandidates) {
    try {
      mysqlPool = mysql.createPool({
        host: hostCandidate,
        port: MYSQL_PORT,
        database: MYSQL_DATABASE,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        waitForConnections: true,
        connectionLimit: 6,
        queueLimit: 0,
        ssl: MYSQL_SSL === "1" || MYSQL_SSL === "true" || MYSQL_SSL === "require"
          ? { rejectUnauthorized: false }
          : undefined
      });
      await mysqlPool.execute("SELECT 1");
      await ensureMySqlRuntimeSchema();
      mysqlRuntimeEnabled = true;
      if (hostCandidate !== MYSQL_HOST) {
        console.warn(`MySQL host fallback in use: ${hostCandidate}`);
      }
      console.log("MySQL runtime persistence is enabled.");
      return;
    } catch (error) {
      lastError = error;
      mysqlRuntimeEnabled = false;
      if (mysqlPool) {
        try {
          await mysqlPool.end();
        } catch (_closeError) {
          // Ignore close failures.
        }
      }
      mysqlPool = null;
    }
  }
  const message = lastError?.message || "Unknown MySQL initialization error.";
  console.error("Failed to initialize MySQL runtime persistence:", message);
}

function buildRoomSnapshotForPg(room) {
  normalizeRoomVideoCollections(room);
  const approvedUsers = room.approvedUsers instanceof Set ? room.approvedUsers : room.members;
  const members = Array.from(room.members).map((username) => ({
    username,
    approved: approvedUsers.has(username)
  }));
  const joinRequests = Array.from(room.joinRequests);
  const sortedMessages = Array.isArray(room.messages)
    ? room.messages
      .slice(-PG_RUNTIME_PERSIST_MAX_MESSAGES_PER_ROOM)
      .sort((a, b) => Number(a?.id || 0) - Number(b?.id || 0))
    : [];
  return {
    room: {
      code: room.code,
      name: room.name,
      host: room.host,
      pendingHostRestore: room.pendingHostRestore || null,
      createdAt: Number(room.createdAt) || Date.now(),
      nextMessageId: Number(room.nextMessageId) || 1,
      video: room.video || null,
      videoSync: room.videoSync || null,
      videoQueue: room.videoQueue || [],
      videoHistory: room.videoHistory || []
    },
    members,
    joinRequests,
    messages: sortedMessages
  };
}

async function loadRuntimeSnapshotFromPg() {
  if (!pgRuntimeEnabled || !pgClient) {
    return;
  }
  try {
    const [sessionsRes, roomsRes, membersRes, requestsRes, messagesRes] = await Promise.all([
      pgClient.query("SELECT token, username, expires_at, is_guest FROM app_runtime_sessions"),
      pgClient.query("SELECT code, name, host, pending_host_restore, created_at, next_message_id, video, video_sync, video_queue, video_history FROM app_runtime_rooms"),
      pgClient.query("SELECT room_code, username, approved FROM app_runtime_room_members"),
      pgClient.query("SELECT room_code, username FROM app_runtime_room_join_requests"),
      pgClient.query("SELECT room_code, message_id, payload FROM app_runtime_room_messages ORDER BY room_code ASC, message_id ASC")
    ]);

    sessions.clear();
    rooms.clear();

    sessionsRes.rows.forEach((row) => {
      const token = String(row?.token || "").trim();
      const username = normalizeUsername(row?.username);
      if (!token || !username) {
        return;
      }
      sessions.set(token, {
        username,
        expiresAt: Number(row?.expires_at) || Date.now() + SESSION_TTL_MS,
        isGuest: Boolean(row?.is_guest)
      });
    });

    const roomByCode = new Map();
    roomsRes.rows.forEach((row) => {
      const code = String(row?.code || "").trim().toUpperCase();
      if (!code) {
        return;
      }
      const room = {
        code,
        name: String(row?.name || "").trim() || "Room",
        host: normalizeUsername(row?.host),
        pendingHostRestore: normalizeUsername(row?.pending_host_restore),
        members: new Set(),
        approvedUsers: new Set(),
        joinRequests: new Set(),
        messageFloorByUser: new Map(),
        clientMessageLog: new Map(),
        messages: [],
        nextMessageId: Math.max(1, Number(row?.next_message_id) || 1),
        createdAt: Number(row?.created_at) || Date.now(),
        video: row?.video || null,
        videoSync: row?.video_sync || null,
        videoQueue: Array.isArray(row?.video_queue) ? row.video_queue : [],
        videoHistory: Array.isArray(row?.video_history) ? row.video_history : []
      };
      roomByCode.set(code, room);
      rooms.set(code, room);
    });

    membersRes.rows.forEach((row) => {
      const code = String(row?.room_code || "").trim().toUpperCase();
      const username = normalizeUsername(row?.username);
      const room = roomByCode.get(code);
      if (!room || !username) {
        return;
      }
      room.members.add(username);
      if (Boolean(row?.approved)) {
        room.approvedUsers.add(username);
      }
      room.messageFloorByUser.set(username, 0);
    });

    requestsRes.rows.forEach((row) => {
      const code = String(row?.room_code || "").trim().toUpperCase();
      const username = normalizeUsername(row?.username);
      const room = roomByCode.get(code);
      if (!room || !username) {
        return;
      }
      room.joinRequests.add(username);
    });

    messagesRes.rows.forEach((row) => {
      const code = String(row?.room_code || "").trim().toUpperCase();
      const room = roomByCode.get(code);
      if (!room) {
        return;
      }
      const payload = row?.payload && typeof row.payload === "object"
        ? row.payload
        : null;
      if (!payload) {
        return;
      }
      const messageId = Number(payload.id || row?.message_id || 0);
      if (!Number.isInteger(messageId) || messageId <= 0) {
        return;
      }
      room.messages.push(payload);
      if (messageId >= room.nextMessageId) {
        room.nextMessageId = messageId + 1;
      }
    });

    rooms.forEach((room) => {
      ensureRoomRuntimeState(room);
      if (!room.host || !room.members.has(room.host)) {
        room.host = Array.from(room.members)[0] || "";
      }
      if (!room.host && room.members.size === 0) {
        rooms.delete(room.code);
      }
    });
    console.log(`Loaded runtime snapshot from PostgreSQL: ${rooms.size} rooms, ${sessions.size} sessions.`);
  } catch (error) {
    console.error("Failed to load runtime snapshot from PostgreSQL:", error.message);
  }
}

async function loadRuntimeSnapshotFromMySql() {
  if (!mysqlRuntimeEnabled || !mysqlPool) {
    return;
  }
  try {
    const [sessionsRows] = await mysqlPool.query("SELECT token, username, expires_at, is_guest FROM app_runtime_sessions");
    const [roomsRows] = await mysqlPool.query("SELECT code, name, host, pending_host_restore, created_at, next_message_id, video, video_sync, video_queue, video_history FROM app_runtime_rooms");
    const [membersRows] = await mysqlPool.query("SELECT room_code, username, approved FROM app_runtime_room_members");
    const [requestsRows] = await mysqlPool.query("SELECT room_code, username FROM app_runtime_room_join_requests");
    const [messagesRows] = await mysqlPool.query("SELECT room_code, message_id, payload FROM app_runtime_room_messages ORDER BY room_code ASC, message_id ASC");

    sessions.clear();
    rooms.clear();

    sessionsRows.forEach((row) => {
      const token = String(row?.token || "").trim();
      const username = normalizeUsername(row?.username);
      if (!token || !username) {
        return;
      }
      sessions.set(token, {
        username,
        expiresAt: Number(row?.expires_at) || Date.now() + SESSION_TTL_MS,
        isGuest: Boolean(Number(row?.is_guest || 0))
      });
    });

    const roomByCode = new Map();
    roomsRows.forEach((row) => {
      const code = String(row?.code || "").trim().toUpperCase();
      if (!code) {
        return;
      }
      const parsedVideo = decodeSqlJson(row?.video, null);
      const parsedVideoSync = decodeSqlJson(row?.video_sync, null);
      const parsedVideoQueue = decodeSqlJson(row?.video_queue, []);
      const parsedVideoHistory = decodeSqlJson(row?.video_history, []);
      const room = {
        code,
        name: String(row?.name || "").trim() || "Room",
        host: normalizeUsername(row?.host),
        pendingHostRestore: normalizeUsername(row?.pending_host_restore),
        members: new Set(),
        approvedUsers: new Set(),
        joinRequests: new Set(),
        messageFloorByUser: new Map(),
        clientMessageLog: new Map(),
        messages: [],
        nextMessageId: Math.max(1, Number(row?.next_message_id) || 1),
        createdAt: Number(row?.created_at) || Date.now(),
        video: parsedVideo && typeof parsedVideo === "object" ? parsedVideo : null,
        videoSync: parsedVideoSync && typeof parsedVideoSync === "object" ? parsedVideoSync : null,
        videoQueue: Array.isArray(parsedVideoQueue) ? parsedVideoQueue : [],
        videoHistory: Array.isArray(parsedVideoHistory) ? parsedVideoHistory : []
      };
      roomByCode.set(code, room);
      rooms.set(code, room);
    });

    membersRows.forEach((row) => {
      const code = String(row?.room_code || "").trim().toUpperCase();
      const username = normalizeUsername(row?.username);
      const room = roomByCode.get(code);
      if (!room || !username) {
        return;
      }
      room.members.add(username);
      if (Boolean(Number(row?.approved || 0))) {
        room.approvedUsers.add(username);
      }
      room.messageFloorByUser.set(username, 0);
    });

    requestsRows.forEach((row) => {
      const code = String(row?.room_code || "").trim().toUpperCase();
      const username = normalizeUsername(row?.username);
      const room = roomByCode.get(code);
      if (!room || !username) {
        return;
      }
      room.joinRequests.add(username);
    });

    messagesRows.forEach((row) => {
      const code = String(row?.room_code || "").trim().toUpperCase();
      const room = roomByCode.get(code);
      if (!room) {
        return;
      }
      const payload = decodeSqlJson(row?.payload, null);
      if (!payload || typeof payload !== "object") {
        return;
      }
      const messageId = Number(payload.id || row?.message_id || 0);
      if (!Number.isInteger(messageId) || messageId <= 0) {
        return;
      }
      room.messages.push(payload);
      if (messageId >= room.nextMessageId) {
        room.nextMessageId = messageId + 1;
      }
    });

    rooms.forEach((room) => {
      ensureRoomRuntimeState(room);
      if (!room.host || !room.members.has(room.host)) {
        room.host = Array.from(room.members)[0] || "";
      }
      if (!room.host && room.members.size === 0) {
        rooms.delete(room.code);
      }
    });
    console.log(`Loaded runtime snapshot from MySQL: ${rooms.size} rooms, ${sessions.size} sessions.`);
  } catch (error) {
    console.error("Failed to load runtime snapshot from MySQL:", error.message);
  }
}

async function persistRuntimeSnapshotToPg() {
  if (!pgRuntimeEnabled || !pgClient) {
    return;
  }
  if (pgRuntimePersistInFlight) {
    pgRuntimePersistQueued = true;
    return;
  }
  pgRuntimePersistInFlight = true;
  try {
    await pgClient.query("BEGIN");
    await pgClient.query("TRUNCATE app_runtime_sessions, app_runtime_rooms, app_runtime_room_members, app_runtime_room_join_requests, app_runtime_room_messages");

    for (const [token, sessionEntry] of sessions.entries()) {
      const meta = getSessionMeta(sessionEntry);
      if (!meta.username) {
        continue;
      }
      await pgClient.query(
        "INSERT INTO app_runtime_sessions (token, username, expires_at, is_guest) VALUES ($1, $2, $3, $4)",
        [token, meta.username, Number(meta.expiresAt || 0), Boolean(meta.isGuest)]
      );
    }

    for (const room of rooms.values()) {
      ensureRoomRuntimeState(room);
      const snapshot = buildRoomSnapshotForPg(room);
      await pgClient.query(
        "INSERT INTO app_runtime_rooms (code, name, host, pending_host_restore, created_at, next_message_id, video, video_sync, video_queue, video_history) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb)",
        [
          snapshot.room.code,
          snapshot.room.name,
          snapshot.room.host,
          snapshot.room.pendingHostRestore || null,
          snapshot.room.createdAt,
          snapshot.room.nextMessageId,
          JSON.stringify(snapshot.room.video || null),
          JSON.stringify(snapshot.room.videoSync || null),
          JSON.stringify(snapshot.room.videoQueue || []),
          JSON.stringify(snapshot.room.videoHistory || [])
        ]
      );

      for (const member of snapshot.members) {
        await pgClient.query(
          "INSERT INTO app_runtime_room_members (room_code, username, approved) VALUES ($1, $2, $3)",
          [snapshot.room.code, member.username, Boolean(member.approved)]
        );
      }
      for (const username of snapshot.joinRequests) {
        await pgClient.query(
          "INSERT INTO app_runtime_room_join_requests (room_code, username) VALUES ($1, $2)",
          [snapshot.room.code, username]
        );
      }
      for (const message of snapshot.messages) {
        await pgClient.query(
          "INSERT INTO app_runtime_room_messages (room_code, message_id, payload) VALUES ($1, $2, $3::jsonb)",
          [snapshot.room.code, Number(message.id || 0), JSON.stringify(message)]
        );
      }
    }
    await pgClient.query("COMMIT");
  } catch (error) {
    try {
      await pgClient.query("ROLLBACK");
    } catch (_rollbackError) {
      // Ignore rollback errors.
    }
    console.error("Failed to persist runtime snapshot to PostgreSQL:", error.message);
  } finally {
    pgRuntimePersistInFlight = false;
    if (pgRuntimePersistQueued) {
      pgRuntimePersistQueued = false;
      setTimeout(() => {
        persistRuntimeSnapshotToSql().catch(() => undefined);
      }, 250);
    }
  }
}

async function persistRuntimeSnapshotToMySql() {
  if (!mysqlRuntimeEnabled || !mysqlPool) {
    return;
  }
  if (pgRuntimePersistInFlight) {
    pgRuntimePersistQueued = true;
    return;
  }
  pgRuntimePersistInFlight = true;
  let connection = null;
  try {
    connection = await mysqlPool.getConnection();
    await connection.beginTransaction();
    await connection.query("DELETE FROM app_runtime_sessions");
    await connection.query("DELETE FROM app_runtime_rooms");
    await connection.query("DELETE FROM app_runtime_room_members");
    await connection.query("DELETE FROM app_runtime_room_join_requests");
    await connection.query("DELETE FROM app_runtime_room_messages");

    for (const [token, sessionEntry] of sessions.entries()) {
      const meta = getSessionMeta(sessionEntry);
      if (!meta.username) {
        continue;
      }
      await connection.execute(
        "INSERT INTO app_runtime_sessions (token, username, expires_at, is_guest) VALUES (?, ?, ?, ?)",
        [token, meta.username, Number(meta.expiresAt || 0), Boolean(meta.isGuest) ? 1 : 0]
      );
    }

    for (const room of rooms.values()) {
      ensureRoomRuntimeState(room);
      const snapshot = buildRoomSnapshotForPg(room);
      await connection.execute(
        "INSERT INTO app_runtime_rooms (code, name, host, pending_host_restore, created_at, next_message_id, video, video_sync, video_queue, video_history) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          snapshot.room.code,
          snapshot.room.name,
          snapshot.room.host,
          snapshot.room.pendingHostRestore || null,
          snapshot.room.createdAt,
          snapshot.room.nextMessageId,
          JSON.stringify(snapshot.room.video || null),
          JSON.stringify(snapshot.room.videoSync || null),
          JSON.stringify(snapshot.room.videoQueue || []),
          JSON.stringify(snapshot.room.videoHistory || [])
        ]
      );

      for (const member of snapshot.members) {
        await connection.execute(
          "INSERT INTO app_runtime_room_members (room_code, username, approved) VALUES (?, ?, ?)",
          [snapshot.room.code, member.username, Boolean(member.approved) ? 1 : 0]
        );
      }
      for (const username of snapshot.joinRequests) {
        await connection.execute(
          "INSERT INTO app_runtime_room_join_requests (room_code, username) VALUES (?, ?)",
          [snapshot.room.code, username]
        );
      }
      for (const message of snapshot.messages) {
        await connection.execute(
          "INSERT INTO app_runtime_room_messages (room_code, message_id, payload) VALUES (?, ?, ?)",
          [snapshot.room.code, Number(message.id || 0), JSON.stringify(message)]
        );
      }
    }
    await connection.commit();
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_rollbackError) {
        // Ignore rollback errors.
      }
    }
    console.error("Failed to persist runtime snapshot to MySQL:", error.message);
  } finally {
    if (connection) {
      connection.release();
    }
    pgRuntimePersistInFlight = false;
    if (pgRuntimePersistQueued) {
      pgRuntimePersistQueued = false;
      setTimeout(() => {
        persistRuntimeSnapshotToSql().catch(() => undefined);
      }, 250);
    }
  }
}

async function initSqlRuntime() {
  if (hasMySqlRuntimeConfig()) {
    await initMySqlRuntime();
    if (!mysqlRuntimeEnabled) {
      await initPgRuntime();
    }
    return;
  }
  await initPgRuntime();
  if (!pgRuntimeEnabled) {
    await initMySqlRuntime();
  }
}

async function loadRuntimeSnapshotFromSql() {
  if (pgRuntimeEnabled) {
    await loadRuntimeSnapshotFromPg();
    return;
  }
  if (mysqlRuntimeEnabled) {
    await loadRuntimeSnapshotFromMySql();
  }
}

async function persistRuntimeSnapshotToSql() {
  if (pgRuntimeEnabled) {
    await persistRuntimeSnapshotToPg();
    return;
  }
  if (mysqlRuntimeEnabled) {
    await persistRuntimeSnapshotToMySql();
  }
}

function scheduleRuntimeSnapshotPersistence() {
  if (!isSqlRuntimeEnabled()) {
    return;
  }
  if (pgRuntimePersistTimer) {
    return;
  }
  pgRuntimePersistTimer = setTimeout(() => {
    pgRuntimePersistTimer = null;
    persistRuntimeSnapshotToSql().catch(() => undefined);
  }, PG_RUNTIME_SNAPSHOT_DEBOUNCE_MS);
}

function setupWebSocketServer(server) {
  wsServer = new WebSocketServer({ noServer: true });
  wsServer.on("connection", (ws, req) => {
    const username = normalizeUsername(req.wsUsername);
    const roomCode = String(req.wsRoomCode || "").trim().toUpperCase();
    if (!username || !roomCode) {
      try {
        ws.close(1008, "invalid-connection");
      } catch (_error) {
        // Ignore close errors.
      }
      return;
    }
    const room = rooms.get(roomCode);
    if (!room || !room.members.has(username)) {
      try {
        ws.close(1008, "forbidden");
      } catch (_error) {
        // Ignore close errors.
      }
      return;
    }
    wsClientMetadata.set(ws, {
      username,
      roomCode,
      joinedAt: Date.now(),
      lastSeenAt: Date.now()
    });
    const subscribers = getRoomSubscriberSet(roomCode, true);
    subscribers.add(ws);
    sendWsJson(ws, {
      type: "connected",
      roomCode,
      timestamp: Date.now()
    });

    ws.on("message", (chunk) => {
      const raw = String(chunk || "").slice(0, 512);
      const meta = wsClientMetadata.get(ws);
      if (meta) {
        meta.lastSeenAt = Date.now();
      }
      if (!raw) {
        return;
      }
      if (raw === "ping") {
        sendWsJson(ws, { type: "pong", timestamp: Date.now() });
        return;
      }
      try {
        const parsed = JSON.parse(raw);
        if (parsed?.type === "ping") {
          sendWsJson(ws, { type: "pong", timestamp: Date.now() });
        }
      } catch (_error) {
        // Ignore malformed client packets.
      }
    });
    ws.on("close", () => {
      removeWsSubscriber(ws);
    });
    ws.on("error", () => {
      removeWsSubscriber(ws);
    });
  });

  server.on("upgrade", (req, socket, head) => {
    const parsed = new URL(req.url, "http://localhost");
    if (parsed.pathname !== WEBSOCKET_PATH) {
      socket.destroy();
      return;
    }
    const token = String(parsed.searchParams.get("token") || "").trim();
    const roomCode = String(parsed.searchParams.get("room") || "").trim().toUpperCase();
    const username = getSessionUsername(token);
    if (!username || !roomCode || !rooms.has(roomCode)) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const room = rooms.get(roomCode);
    if (!room.members.has(username)) {
      socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    req.wsUsername = username;
    req.wsRoomCode = roomCode;
    wsServer.handleUpgrade(req, socket, head, (ws) => {
      wsServer.emit("connection", ws, req);
    });
  });

  wsPingTimer = setInterval(() => {
    roomSocketSubscribers.forEach((subscribers) => {
      Array.from(subscribers).forEach((ws) => {
        if (!ws || ws.readyState !== ws.OPEN) {
          removeWsSubscriber(ws);
          return;
        }
        try {
          ws.ping();
        } catch (_error) {
          removeWsSubscriber(ws);
        }
      });
    });
    pruneRateLimitBuckets();
  }, WS_PING_INTERVAL_MS);
}

function getGitHubUsersRepoParts() {
  const normalized = String(GITHUB_USERS_SYNC_REPO || "")
    .replace(/^https?:\/\/github\.com\//i, "")
    .replace(/\.git$/i, "")
    .replace(/^\/+|\/+$/g, "");
  const match = normalized.match(/^([^/\s]+)\/([^/\s]+)$/);
  if (!match) {
    return null;
  }
  return { owner: match[1], repo: match[2] };
}

function isGitHubUsersSyncEnabled() {
  return Boolean(GITHUB_USERS_SYNC_TOKEN && getGitHubUsersRepoParts());
}

function getGitHubUsersContentApiPath(includeRef = false) {
  const repoParts = getGitHubUsersRepoParts();
  if (!repoParts) {
    return "";
  }
  const encodedOwner = encodeURIComponent(repoParts.owner);
  const encodedRepo = encodeURIComponent(repoParts.repo);
  const encodedFilePath = GITHUB_USERS_SYNC_PATH
    .split("/")
    .filter(Boolean)
    .map((part) => encodeURIComponent(part))
    .join("/");
  if (!encodedFilePath) {
    return "";
  }
  let apiPath = `/repos/${encodedOwner}/${encodedRepo}/contents/${encodedFilePath}`;
  if (includeRef) {
    apiPath += `?ref=${encodeURIComponent(GITHUB_USERS_SYNC_BRANCH)}`;
  }
  return apiPath;
}

function githubApiRequestJson(method, apiPath, payload = null) {
  return new Promise((resolve, reject) => {
    const body = payload === null ? "" : JSON.stringify(payload);
    const req = https.request(
      {
        hostname: "api.github.com",
        method,
        path: apiPath,
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${GITHUB_USERS_SYNC_TOKEN}`,
          "User-Agent": "sawawatch-users-sync",
          "X-GitHub-Api-Version": "2022-11-28",
          ...(body
            ? {
              "Content-Type": "application/json",
              "Content-Length": Buffer.byteLength(body)
            }
            : {})
        }
      },
      (res) => {
        let raw = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          let parsed = null;
          if (raw) {
            try {
              parsed = JSON.parse(raw);
            } catch (_error) {
              parsed = null;
            }
          }
          resolve({
            statusCode: Number(res.statusCode || 0),
            body: raw,
            json: parsed
          });
        });
      }
    );
    req.setTimeout(GITHUB_USERS_SYNC_TIMEOUT_MS, () => {
      req.destroy(new Error("GitHub API timeout"));
    });
    req.on("error", (error) => {
      reject(error);
    });
    if (body) {
      req.write(body);
    }
    req.end();
  });
}

async function fetchUsersFromGitHub() {
  if (!isGitHubUsersSyncEnabled()) {
    return null;
  }
  const apiPath = getGitHubUsersContentApiPath(true);
  if (!apiPath) {
    return null;
  }
  const response = await githubApiRequestJson("GET", apiPath);
  if (response.statusCode === 404) {
    return null;
  }
  if (response.statusCode !== 200) {
    const reason = response.json?.message || response.body || `status ${response.statusCode}`;
    throw new Error(`GitHub users fetch failed: ${reason}`);
  }
  const encodedContent = String(response.json?.content || "").replace(/\s+/g, "");
  if (!encodedContent) {
    throw new Error("GitHub users file content is empty.");
  }
  const decodedContent = Buffer.from(encodedContent, "base64").toString("utf8");
  const parsed = parseJsonWithOptionalBom(decodedContent);
  if (!Array.isArray(parsed)) {
    throw new Error("GitHub users file must contain a JSON array.");
  }
  const sha = typeof response.json?.sha === "string" ? response.json.sha : "";
  return { list: parsed, sha };
}

async function fetchGitHubUsersFileSha() {
  if (!isGitHubUsersSyncEnabled()) {
    return "";
  }
  const apiPath = getGitHubUsersContentApiPath(true);
  if (!apiPath) {
    return "";
  }
  const response = await githubApiRequestJson("GET", apiPath);
  if (response.statusCode === 404) {
    return "";
  }
  if (response.statusCode !== 200) {
    const reason = response.json?.message || response.body || `status ${response.statusCode}`;
    throw new Error(`GitHub users sha fetch failed: ${reason}`);
  }
  return typeof response.json?.sha === "string" ? response.json.sha : "";
}

async function pushUsersToGitHub(serializedUsers) {
  if (!isGitHubUsersSyncEnabled()) {
    return;
  }
  const apiPath = getGitHubUsersContentApiPath(false);
  if (!apiPath) {
    return;
  }
  const sha = gitHubUsersFileSha || await fetchGitHubUsersFileSha();
  const payload = {
    message: `chore(data): sync users.json (${new Date().toISOString()})`,
    content: Buffer.from(String(serializedUsers || ""), "utf8").toString("base64"),
    branch: GITHUB_USERS_SYNC_BRANCH
  };
  if (sha) {
    payload.sha = sha;
  }
  const response = await githubApiRequestJson("PUT", apiPath, payload);
  if (response.statusCode !== 200 && response.statusCode !== 201) {
    const reason = response.json?.message || response.body || `status ${response.statusCode}`;
    throw new Error(`GitHub users sync failed: ${reason}`);
  }
  gitHubUsersFileSha = String(response.json?.content?.sha || payload.sha || "");
}

function queueUsersSyncToGitHub(serializedUsers) {
  if (!isGitHubUsersSyncEnabled()) {
    return;
  }
  gitHubUsersSyncQueue = gitHubUsersSyncQueue
    .catch(() => undefined)
    .then(() => pushUsersToGitHub(serializedUsers))
    .catch((error) => {
      console.error("Failed to sync users to GitHub:", error.message);
    });
}

function normalizePathForCompare(dirPath) {
  return path.resolve(String(dirPath || ""))
    .replace(/\\/g, "/")
    .replace(/\/+$/, "")
    .toLowerCase();
}

const OS_TMP_DIR_NORMALIZED = normalizePathForCompare(os.tmpdir());

function isLikelyEphemeralDir(dirPath) {
  const normalized = normalizePathForCompare(dirPath);
  if (!normalized) {
    return false;
  }
  return (
    normalized === "/tmp"
    || normalized.startsWith("/tmp/")
    || normalized === "/var/tmp"
    || normalized.startsWith("/var/tmp/")
    || normalized === OS_TMP_DIR_NORMALIZED
    || normalized.startsWith(`${OS_TMP_DIR_NORMALIZED}/`)
  );
}

function hasDurableUsersStorage() {
  if (!isLikelyEphemeralDir(DATA_DIR)) {
    return true;
  }
  if (isGitHubUsersSyncEnabled()) {
    return true;
  }
  if (DATABASE_URL || hasMySqlRuntimeConfig()) {
    return true;
  }
  return false;
}

function warnIfUsersStorageIsEphemeral() {
  if (hasDurableUsersStorage()) {
    return;
  }
  console.warn(
    [
      "Users storage path appears ephemeral:",
      DATA_DIR,
      "Created accounts may be lost after restart/deploy.",
      "Use a persistent disk path (e.g. /var/data/...) or enable GITHUB_USERS_SYNC / DATABASE_URL / MYSQL_*."
    ].join(" ")
  );
}

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function writeTextFileAtomic(filePath, text) {
  const target = String(filePath || "").trim();
  if (!target) {
    throw new Error("Target file path is missing.");
  }
  const tempFile = `${target}.${process.pid}.${Date.now()}.tmp`;
  try {
    fs.writeFileSync(tempFile, String(text || ""), "utf8");
    fs.renameSync(tempFile, target);
  } finally {
    if (fs.existsSync(tempFile)) {
      try {
        fs.unlinkSync(tempFile);
      } catch (_error) {
        // Ignore temp cleanup errors.
      }
    }
  }
}

function ensureUploadsDir() {
  const ensureWritableDir = (directoryPath) => {
    fs.mkdirSync(directoryPath, { recursive: true });
    fs.accessSync(directoryPath, fs.constants.R_OK | fs.constants.W_OK);
  };
  try {
    ensureWritableDir(ROOM_VIDEO_UPLOAD_DIR_DEFAULT);
    activeRoomVideoUploadDir = ROOM_VIDEO_UPLOAD_DIR_DEFAULT;
    return true;
  } catch (error) {
    console.warn("Default uploads directory is not writable:", error.message);
  }
  try {
    ensureWritableDir(ROOM_VIDEO_UPLOAD_DIR_FALLBACK);
    activeRoomVideoUploadDir = ROOM_VIDEO_UPLOAD_DIR_FALLBACK;
    console.warn("Using fallback runtime uploads directory:", ROOM_VIDEO_UPLOAD_DIR_FALLBACK);
    return true;
  } catch (fallbackError) {
    console.warn("Fallback uploads directory is not writable:", fallbackError.message);
    return false;
  }
}

function getRoomVideoUploadDir() {
  return activeRoomVideoUploadDir;
}

function removeFileIfExists(filePath) {
  const target = String(filePath || "").trim();
  if (!target) {
    return;
  }
  try {
    if (fs.existsSync(target)) {
      fs.unlinkSync(target);
    }
  } catch (_error) {
    // Ignore cleanup errors.
  }
}

function hydrateUsersFromList(list) {
  users.clear();
  list.forEach((item) => {
    if (!item || typeof item.username !== "string" || typeof item.passwordHash !== "string") {
      return;
    }
    const username = item.username.trim().toLowerCase();
    if (!username) {
      return;
    }
    users.set(username, {
      passwordHash: item.passwordHash,
      passwordSalt: typeof item.passwordSalt === "string" ? item.passwordSalt : "",
      createdAt: Number(item.createdAt) || Date.now(),
      displayName: normalizeDisplayName(item.displayName, username),
      avatarDataUrl: typeof item.avatarDataUrl === "string" ? item.avatarDataUrl : "",
      countryCode: normalizeCountryCode(item.countryCode)
    });
  });
}

function buildRegisteredUsersList() {
  return Array.from(users.entries()).map(([username, info]) => ({
    username,
    passwordHash: info.passwordHash,
    passwordSalt: typeof info.passwordSalt === "string" ? info.passwordSalt : "",
    createdAt: Number(info.createdAt) || Date.now(),
    displayName: normalizeDisplayName(info.displayName, username),
    avatarDataUrl: info.avatarDataUrl || "",
    countryCode: normalizeCountryCode(info.countryCode)
  }));
}

function writeUsersListToLocalFiles(list) {
  ensureDataDir();
  const serialized = JSON.stringify(Array.isArray(list) ? list : [], null, 2);
  writeTextFileAtomic(USERS_FILE, serialized);
  writeTextFileAtomic(USERS_BACKUP_FILE, serialized);
  return serialized;
}

async function loadUsersFromPg() {
  if (!pgRuntimeEnabled || !pgClient) {
    return -1;
  }
  const result = await pgClient.query(
    "SELECT username, password_hash, password_salt, created_at, display_name, avatar_data_url, country_code FROM app_users ORDER BY username ASC"
  );
  const list = result.rows.map((row) => ({
    username: normalizeUsername(row?.username),
    passwordHash: String(row?.password_hash || ""),
    passwordSalt: String(row?.password_salt || ""),
    createdAt: Number(row?.created_at) || Date.now(),
    displayName: String(row?.display_name || ""),
    avatarDataUrl: String(row?.avatar_data_url || ""),
    countryCode: normalizeCountryCode(row?.country_code)
  }));
  hydrateUsersFromList(list);
  return list.length;
}

async function loadUsersFromMySql() {
  if (!mysqlRuntimeEnabled || !mysqlPool) {
    return -1;
  }
  const [rows] = await mysqlPool.query(
    "SELECT username, password_hash, password_salt, created_at, display_name, avatar_data_url, country_code FROM app_users ORDER BY username ASC"
  );
  const list = rows.map((row) => ({
    username: normalizeUsername(row?.username),
    passwordHash: String(row?.password_hash || ""),
    passwordSalt: String(row?.password_salt || ""),
    createdAt: Number(row?.created_at) || Date.now(),
    displayName: String(row?.display_name || ""),
    avatarDataUrl: String(row?.avatar_data_url || ""),
    countryCode: normalizeCountryCode(row?.country_code)
  }));
  hydrateUsersFromList(list);
  return list.length;
}

async function loadUsersFromSql() {
  if (pgRuntimeEnabled) {
    return loadUsersFromPg();
  }
  if (mysqlRuntimeEnabled) {
    return loadUsersFromMySql();
  }
  return -1;
}

async function replaceUsersSnapshotInPg(list) {
  if (!pgRuntimeEnabled || !pgClient) {
    return;
  }
  await pgClient.query("BEGIN");
  try {
    await pgClient.query("TRUNCATE app_users");
    for (const item of list) {
      await pgClient.query(
        "INSERT INTO app_users (username, password_hash, password_salt, created_at, display_name, avatar_data_url, country_code) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        [
          item.username,
          item.passwordHash,
          item.passwordSalt || "",
          Number(item.createdAt) || Date.now(),
          normalizeDisplayName(item.displayName, item.username),
          item.avatarDataUrl || "",
          normalizeCountryCode(item.countryCode)
        ]
      );
    }
    await pgClient.query("COMMIT");
  } catch (error) {
    try {
      await pgClient.query("ROLLBACK");
    } catch (_rollbackError) {
      // Ignore rollback errors.
    }
    throw error;
  }
}

async function replaceUsersSnapshotInMySql(list) {
  if (!mysqlRuntimeEnabled || !mysqlPool) {
    return;
  }
  const connection = await mysqlPool.getConnection();
  try {
    await connection.beginTransaction();
    await connection.query("DELETE FROM app_users");
    for (const item of list) {
      await connection.execute(
        "INSERT INTO app_users (username, password_hash, password_salt, created_at, display_name, avatar_data_url, country_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
          item.username,
          item.passwordHash,
          item.passwordSalt || "",
          Number(item.createdAt) || Date.now(),
          normalizeDisplayName(item.displayName, item.username),
          item.avatarDataUrl || "",
          normalizeCountryCode(item.countryCode)
        ]
      );
    }
    await connection.commit();
  } catch (error) {
    try {
      await connection.rollback();
    } catch (_rollbackError) {
      // Ignore rollback errors.
    }
    throw error;
  } finally {
    connection.release();
  }
}

async function replaceUsersSnapshotInSql(list) {
  if (pgRuntimeEnabled) {
    await replaceUsersSnapshotInPg(list);
    return;
  }
  if (mysqlRuntimeEnabled) {
    await replaceUsersSnapshotInMySql(list);
  }
}

async function persistUsersSnapshotToSql() {
  if (!isSqlRuntimeEnabled()) {
    return;
  }
  if (usersSqlPersistInFlight) {
    usersSqlPersistQueued = true;
    return;
  }
  usersSqlPersistInFlight = true;
  try {
    const list = buildRegisteredUsersList();
    await replaceUsersSnapshotInSql(list);
  } catch (error) {
    console.error("Failed to persist users to SQL storage:", error.message);
  } finally {
    usersSqlPersistInFlight = false;
    if (usersSqlPersistQueued) {
      usersSqlPersistQueued = false;
      setTimeout(() => {
        persistUsersSnapshotToSql().catch(() => undefined);
      }, 250);
    }
  }
}

function scheduleUsersSnapshotPersistenceToSql() {
  if (!isSqlRuntimeEnabled()) {
    return;
  }
  if (usersSqlPersistTimer) {
    return;
  }
  usersSqlPersistTimer = setTimeout(() => {
    usersSqlPersistTimer = null;
    persistUsersSnapshotToSql().catch(() => undefined);
  }, 240);
}

async function loadUsers() {
  ensureDataDir();
  if (isSqlRuntimeEnabled()) {
    try {
      const sqlCount = await loadUsersFromSql();
      if (sqlCount > 0) {
        writeUsersListToLocalFiles(buildRegisteredUsersList());
        console.log(`Loaded ${sqlCount} users from SQL storage.`);
        return;
      }
      console.log("No users found in SQL storage. Falling back to GitHub/local files.");
    } catch (error) {
      console.error("Failed to load users from SQL storage:", error.message);
      console.warn("Falling back to GitHub/local users files.");
    }
  }

  let loadedFromFallback = false;
  if (isGitHubUsersSyncEnabled()) {
    try {
      const remoteUsers = await fetchUsersFromGitHub();
      if (remoteUsers && Array.isArray(remoteUsers.list)) {
        hydrateUsersFromList(remoteUsers.list);
        writeUsersListToLocalFiles(buildRegisteredUsersList());
        gitHubUsersFileSha = String(remoteUsers.sha || "");
        console.log(`Loaded ${remoteUsers.list.length} users from GitHub sync.`);
        loadedFromFallback = true;
      }
    } catch (error) {
      console.error("Failed to load users from GitHub sync:", error.message);
      console.warn("Falling back to local users files.");
    }
  }
  if (!loadedFromFallback) {
    try {
      if (!fs.existsSync(USERS_FILE)) {
        writeUsersListToLocalFiles([]);
        loadedFromFallback = true;
      } else {
        const raw = fs.readFileSync(USERS_FILE, "utf8");
        const list = parseJsonWithOptionalBom(raw);
        if (!Array.isArray(list)) {
          throw new Error("users.json must contain an array.");
        }
        hydrateUsersFromList(list);
        writeTextFileAtomic(USERS_BACKUP_FILE, JSON.stringify(list, null, 2));
        loadedFromFallback = true;
      }
    } catch (error) {
      console.error("Failed to load users from primary file:", error.message);
      try {
        if (!fs.existsSync(USERS_BACKUP_FILE)) {
          throw new Error("backup file not found.");
        }
        const backupRaw = fs.readFileSync(USERS_BACKUP_FILE, "utf8");
        const backupList = parseJsonWithOptionalBom(backupRaw);
        if (!Array.isArray(backupList)) {
          throw new Error("backup users file must contain an array.");
        }
        hydrateUsersFromList(backupList);
        writeTextFileAtomic(USERS_FILE, JSON.stringify(backupList, null, 2));
        console.warn("Recovered users from backup file.");
        loadedFromFallback = true;
      } catch (backupError) {
        console.error("Failed to recover users from backup:", backupError.message);
      }
    }
  }

  if (isSqlRuntimeEnabled() && loadedFromFallback && users.size > 0) {
    try {
      await replaceUsersSnapshotInSql(buildRegisteredUsersList());
      console.log(`Migrated ${users.size} users to SQL storage.`);
    } catch (error) {
      console.error("Failed to migrate users to SQL storage:", error.message);
    }
  } else if (isSqlRuntimeEnabled() && loadedFromFallback && users.size === 0) {
    try {
      await replaceUsersSnapshotInSql([]);
    } catch (error) {
      console.error("Failed to clear SQL users snapshot:", error.message);
    }
  }
}

function saveUsers() {
  try {
    const list = buildRegisteredUsersList();
    const serialized = writeUsersListToLocalFiles(list);
    queueUsersSyncToGitHub(serialized);
    scheduleUsersSnapshotPersistenceToSql();
    return true;
  } catch (error) {
    console.error("Failed to save users:", error.message);
    return false;
  }
}

function loadModeration() {
  try {
    ensureDataDir();
    if (!fs.existsSync(MODERATION_FILE)) {
      fs.writeFileSync(
        MODERATION_FILE,
        JSON.stringify({ bannedUsers: [], unbanRequests: [], siteAnnouncement: null }, null, 2),
        "utf8"
      );
      return;
    }
    const raw = fs.readFileSync(MODERATION_FILE, "utf8");
    const parsed = parseJsonWithOptionalBom(raw);
    const bannedList = Array.isArray(parsed?.bannedUsers) ? parsed.bannedUsers : [];
    const requestList = Array.isArray(parsed?.unbanRequests) ? parsed.unbanRequests : [];
    const savedAnnouncement = parsed?.siteAnnouncement;

    bannedList.forEach((entry) => {
      if (!entry || typeof entry.username !== "string") {
        return;
      }
      const username = entry.username.toLowerCase();
      bannedUsers.set(username, {
        username,
        reason: String(entry.reason || ""),
        bannedBy: String(entry.bannedBy || SUPERVISOR_USERNAME),
        bannedAt: Number(entry.bannedAt) || Date.now()
      });
    });

    requestList.forEach((entry) => {
      if (!entry || typeof entry.username !== "string") {
        return;
      }
      const username = entry.username.toLowerCase();
      unbanRequests.set(username, {
        username,
        reason: String(entry.reason || ""),
        status: String(entry.status || "pending"),
        createdAt: Number(entry.createdAt) || Date.now(),
        updatedAt: Number(entry.updatedAt) || Date.now(),
        reviewedBy: String(entry.reviewedBy || ""),
        reviewNote: String(entry.reviewNote || "")
      });
    });

    siteAnnouncement = formatSiteAnnouncement(savedAnnouncement);
  } catch (error) {
    console.error("Failed to load moderation:", error.message);
  }
}

function saveModeration() {
  try {
    ensureDataDir();
    const payload = {
      bannedUsers: Array.from(bannedUsers.values()),
      unbanRequests: Array.from(unbanRequests.values()),
      siteAnnouncement: siteAnnouncement ? { ...siteAnnouncement } : null
    };
    fs.writeFileSync(MODERATION_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (error) {
    console.error("Failed to save moderation:", error.message);
  }
}

function normalizeYouTubeCacheEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }
  const videoId = String(entry.videoId || "").trim();
  if (!YOUTUBE_VIDEO_ID_REGEX.test(videoId)) {
    return null;
  }
  const storedFileName = path.basename(String(entry.storedFileName || "").trim());
  if (!storedFileName) {
    return null;
  }
  const cleanFileName = sanitizeUploadedFilename(entry.cleanFileName || `youtube-${videoId}`);
  const durationSec = normalizeRoomVideoDuration(entry.durationSec);
  const downloadedBytes = Math.max(0, Number(entry.downloadedBytes || 0));
  const mimeType = normalizeVideoMimeType(entry.mimeType || "video/mp4") || "video/mp4";
  const downloadedAt = Number(entry.downloadedAt) || Date.now();
  const lastUsedAt = Number(entry.lastUsedAt) || downloadedAt;
  const hitCount = Math.max(0, Number(entry.hitCount || 0));
  return {
    videoId,
    storedFileName,
    cleanFileName,
    durationSec,
    downloadedBytes,
    mimeType,
    downloadedAt,
    lastUsedAt,
    hitCount
  };
}

function isStoredVideoFileUsedInAnyRoom(storedFileName) {
  const normalized = String(storedFileName || "").trim();
  if (!normalized) {
    return false;
  }
  const publicSrc = `${ROOM_VIDEO_PUBLIC_PREFIX}${normalized}`;
  for (const room of rooms.values()) {
    if (String(room?.video?.src || "") === publicSrc) {
      return true;
    }
  }
  return false;
}

function removeCachedVideoFileIfSafe(storedFileName) {
  if (!storedFileName || isStoredVideoFileUsedInAnyRoom(storedFileName)) {
    return;
  }
  const absolutePath = path.join(getRoomVideoUploadDir(), storedFileName);
  if (!fs.existsSync(absolutePath)) {
    return;
  }
  try {
    fs.unlinkSync(absolutePath);
  } catch (_error) {
    // Ignore cleanup errors.
  }
}

function saveYouTubeCache() {
  try {
    ensureDataDir();
    const list = Array.from(youtubeVideoCache.values()).sort((a, b) => Number(b.lastUsedAt || 0) - Number(a.lastUsedAt || 0));
    fs.writeFileSync(YOUTUBE_CACHE_FILE, JSON.stringify(list, null, 2), "utf8");
  } catch (error) {
    console.error("Failed to save YouTube cache:", error.message);
  }
}

function pruneYouTubeCache() {
  const now = Date.now();
  const entries = Array.from(youtubeVideoCache.values());

  for (const entry of entries) {
    const absolutePath = path.join(getRoomVideoUploadDir(), entry.storedFileName);
    if (!fs.existsSync(absolutePath)) {
      youtubeVideoCache.delete(entry.videoId);
      continue;
    }
    const ageMs = now - Number(entry.lastUsedAt || entry.downloadedAt || now);
    if (ageMs > YOUTUBE_CACHE_MAX_AGE_MS && !isStoredVideoFileUsedInAnyRoom(entry.storedFileName)) {
      removeCachedVideoFileIfSafe(entry.storedFileName);
      youtubeVideoCache.delete(entry.videoId);
    }
  }

  if (youtubeVideoCache.size > YOUTUBE_CACHE_MAX_ITEMS) {
    const overflow = youtubeVideoCache.size - YOUTUBE_CACHE_MAX_ITEMS;
    const sorted = Array.from(youtubeVideoCache.values()).sort((a, b) => Number(a.lastUsedAt || 0) - Number(b.lastUsedAt || 0));
    let removed = 0;
    for (const entry of sorted) {
      if (removed >= overflow) {
        break;
      }
      if (isStoredVideoFileUsedInAnyRoom(entry.storedFileName)) {
        continue;
      }
      removeCachedVideoFileIfSafe(entry.storedFileName);
      youtubeVideoCache.delete(entry.videoId);
      removed += 1;
    }
  }
}

function loadYouTubeCache() {
  try {
    ensureDataDir();
    ensureUploadsDir();
    if (!fs.existsSync(YOUTUBE_CACHE_FILE)) {
      fs.writeFileSync(YOUTUBE_CACHE_FILE, "[]", "utf8");
      return;
    }
    const raw = fs.readFileSync(YOUTUBE_CACHE_FILE, "utf8");
    const list = parseJsonWithOptionalBom(raw);
    if (!Array.isArray(list)) {
      return;
    }
    youtubeVideoCache.clear();
    for (const item of list) {
      const normalized = normalizeYouTubeCacheEntry(item);
      if (!normalized) {
        continue;
      }
      youtubeVideoCache.set(normalized.videoId, normalized);
    }
    pruneYouTubeCache();
    saveYouTubeCache();
  } catch (error) {
    console.error("Failed to load YouTube cache:", error.message);
  }
}

function getStoredFileNameFromPublicSrc(src) {
  const clean = String(src || "").trim();
  if (!clean.startsWith(ROOM_VIDEO_PUBLIC_PREFIX)) {
    return "";
  }
  const value = path.basename(clean.slice(ROOM_VIDEO_PUBLIC_PREFIX.length));
  return value || "";
}

function isVideoSrcInYouTubeCache(src) {
  const storedFileName = getStoredFileNameFromPublicSrc(src);
  if (!storedFileName) {
    return false;
  }
  for (const entry of youtubeVideoCache.values()) {
    if (entry.storedFileName === storedFileName) {
      return true;
    }
  }
  return false;
}

function getCachedYouTubeVideo(videoId) {
  const cleanVideoId = String(videoId || "").trim();
  if (!YOUTUBE_VIDEO_ID_REGEX.test(cleanVideoId)) {
    return null;
  }
  pruneYouTubeCache();
  const entry = youtubeVideoCache.get(cleanVideoId);
  if (!entry) {
    return null;
  }
  const absolutePath = path.join(getRoomVideoUploadDir(), entry.storedFileName);
  if (!fs.existsSync(absolutePath)) {
    youtubeVideoCache.delete(cleanVideoId);
    saveYouTubeCache();
    return null;
  }
  entry.lastUsedAt = Date.now();
  entry.hitCount = Math.max(0, Number(entry.hitCount || 0)) + 1;
  youtubeVideoCache.set(cleanVideoId, entry);
  saveYouTubeCache();
  return { ...entry };
}

function rememberYouTubeCache(videoId, downloaded) {
  const cleanVideoId = String(videoId || "").trim();
  if (!YOUTUBE_VIDEO_ID_REGEX.test(cleanVideoId) || !downloaded) {
    return null;
  }
  const entry = normalizeYouTubeCacheEntry({
    videoId: cleanVideoId,
    storedFileName: downloaded.storedFileName,
    cleanFileName: downloaded.cleanFileName,
    durationSec: downloaded.durationSec,
    downloadedBytes: downloaded.downloadedBytes,
    mimeType: downloaded.mimeType,
    downloadedAt: Date.now(),
    lastUsedAt: Date.now(),
    hitCount: 1
  });
  if (!entry) {
    return null;
  }
  youtubeVideoCache.set(cleanVideoId, entry);
  pruneYouTubeCache();
  saveYouTubeCache();
  return { ...entry };
}

function extractYouTubeProxyIdFromSrc(src) {
  const clean = String(src || "").trim();
  const prefix = "/api/youtube-proxy/";
  if (!clean.startsWith(prefix)) {
    return "";
  }
  const value = clean.slice(prefix.length).split(/[?#]/)[0] || "";
  if (!/^[A-Za-z0-9]+$/.test(value)) {
    return "";
  }
  return value;
}

function pruneYouTubeProxyStreams(now = Date.now()) {
  youtubeProxyStreams.forEach((item, id) => {
    if (!item || Number(item.expiresAt || 0) <= now) {
      youtubeProxyStreams.delete(id);
    }
  });
}

function registerYouTubeProxyStream(videoId, direct) {
  pruneYouTubeProxyStreams();
  const id = randomToken().slice(0, 24);
  youtubeProxyStreams.set(id, {
    id,
    videoId: String(videoId || ""),
    preferredItag: Number(direct?.itag || 0),
    mimeType: String(direct?.mimeType || "video/mp4"),
    estimatedBytes: Number(direct?.estimatedBytes || 0),
    createdAt: Date.now(),
    expiresAt: Date.now() + YOUTUBE_PROXY_TTL_MS
  });
  return {
    streamId: id,
    src: `/api/youtube-proxy/${id}`
  };
}

function getYouTubeProxyStream(streamId) {
  pruneYouTubeProxyStreams();
  const key = String(streamId || "").trim();
  const entry = youtubeProxyStreams.get(key);
  if (!entry) {
    return null;
  }
  if (!YOUTUBE_VIDEO_ID_REGEX.test(String(entry.videoId || ""))) {
    youtubeProxyStreams.delete(key);
    return null;
  }
  entry.expiresAt = Date.now() + YOUTUBE_PROXY_TTL_MS;
  return entry;
}

async function serveYouTubeProxyStream(req, res, streamId) {
  const item = getYouTubeProxyStream(streamId);
  if (!item) {
    sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½. ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Video stream link expired. Please set the video again.") });
    return;
  }

  const method = req.method === "HEAD" ? "HEAD" : "GET";
  const rangeHeader = String(req.headers.range || "").trim();

  let info;
  try {
    info = await getYouTubeInfoRobust(item.videoId);
  } catch (error) {
    sendJson(res, 502, {
      error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Could not proxy YouTube video right now.")
    });
    return;
  }

  const playable = selectYouTubePlayableFormats(info);
  let format = null;
  if (item.preferredItag > 0) {
    format = playable.find((entry) => Number(entry?.itag || 0) === item.preferredItag) || null;
  }
  if (!format) {
    format = playable[0] || null;
  }
  if (!format) {
    sendJson(res, 415, {
      error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Could not find a compatible playable format.")
    });
    return;
  }

  const mimeType = String(format?.mimeType || item.mimeType || "video/mp4").split(";")[0].trim() || "video/mp4";
  const totalSize = Number(format?.contentLength || 0);
  const headers = {
    "content-type": mimeType,
    "cache-control": "no-store"
  };

  let statusCode = 200;
  let range = null;
  if (rangeHeader && totalSize > 0) {
    const match = rangeHeader.match(/^bytes=(\d*)-(\d*)$/i);
    if (!match) {
      res.writeHead(
        416,
        securityHeaders({
          "content-range": `bytes */${totalSize}`,
          "cache-control": "no-store"
        })
      );
      res.end();
      return;
    }
    const start = match[1] === "" ? 0 : Number(match[1]);
    const end = match[2] === "" ? totalSize - 1 : Number(match[2]);
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end < start || start >= totalSize) {
      res.writeHead(
        416,
        securityHeaders({
          "content-range": `bytes */${totalSize}`,
          "cache-control": "no-store"
        })
      );
      res.end();
      return;
    }
    const safeEnd = Math.min(totalSize - 1, end);
    range = { start, end: safeEnd };
    statusCode = 206;
    headers["accept-ranges"] = "bytes";
    headers["content-range"] = `bytes ${start}-${safeEnd}/${totalSize}`;
    headers["content-length"] = String(safeEnd - start + 1);
  } else if (totalSize > 0) {
    headers["accept-ranges"] = "bytes";
    headers["content-length"] = String(totalSize);
  }

  res.writeHead(statusCode, securityHeaders(headers));
  if (method === "HEAD") {
    res.end();
    return;
  }

  const streamOptions = { format };
  if (range) {
    streamOptions.range = range;
  }

  const stream = ytdl.downloadFromInfo(info, streamOptions);
  let completed = false;
  const fail = () => {
    if (completed) {
      return;
    }
    completed = true;
    if (!res.writableEnded) {
      res.end();
    }
  };

  stream.on("error", fail);
  stream.on("end", () => {
    completed = true;
  });
  req.on("close", () => {
    if (!stream.destroyed) {
      stream.destroy();
    }
  });
  stream.pipe(res);
}

function getLang(req) {
  return req.headers["x-lang"] === "en" ? "en" : "ar";
}

function looksLikeMojibake(text) {
  if (typeof text !== "string" || text.length < 8) {
    return false;
  }
  // Detect common mojibake by spotting Arabic letters mixed with Latin-1 bytes.
  const suspicious = text.match(/[\u0600-\u06FF][\u00A0-\u00FF]|[\u00A0-\u00FF][\u0600-\u06FF]/g);
  return Boolean(suspicious && suspicious.length >= 3 && suspicious.length / text.length >= 0.12);
}

const WINDOWS_1256_CHAR_TO_BYTE = (() => {
  try {
    const decoder = new TextDecoder("windows-1256");
    const map = new Map();
    for (let byte = 0; byte < 256; byte += 1) {
      const char = decoder.decode(Uint8Array.from([byte]));
      if (!map.has(char)) {
        map.set(char, byte);
      }
    }
    return map;
  } catch (_error) {
    return null;
  }
})();

const UTF8_TEXT_DECODER = (() => {
  try {
    return new TextDecoder("utf-8");
  } catch (_error) {
    return null;
  }
})();

function repairArabicMojibake(text) {
  const value = String(text || "");
  if (!looksLikeMojibake(value) || !WINDOWS_1256_CHAR_TO_BYTE || !UTF8_TEXT_DECODER) {
    return value;
  }
  const bytes = [];
  for (const ch of value) {
    const byte = WINDOWS_1256_CHAR_TO_BYTE.get(ch);
    if (byte === undefined) {
      return value;
    }
    bytes.push(byte);
  }
  let repaired = value;
  try {
    repaired = UTF8_TEXT_DECODER.decode(Uint8Array.from(bytes));
  } catch (_error) {
    return value;
  }
  if (!repaired || looksLikeMojibake(repaired)) {
    return value;
  }
  return repaired;
}

function normalizeDisplayName(value, fallbackUsername = "") {
  const fallback = String(fallbackUsername || "").trim().toLowerCase().slice(0, 30);
  const clean = String(value || "").trim().slice(0, 30);
  if (!clean || looksLikeMojibake(clean)) {
    return fallback || SUPERVISOR_USERNAME;
  }
  return clean;
}

function normalizeCountryCode(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (!COUNTRY_CODE_REGEX.test(clean) || NON_ISO_COUNTRY_CODES.has(clean)) {
    return "";
  }
  return clean;
}

function extractCountryCodeFromLocaleTag(rawTag) {
  const tag = String(rawTag || "").trim();
  if (!tag) {
    return "";
  }
  const subTags = tag.split(/[-_]/g);
  if (subTags.length < 2) {
    return "";
  }
  for (let i = 1; i < subTags.length; i += 1) {
    const piece = String(subTags[i] || "").trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(piece)) {
      continue;
    }
    const normalized = normalizeCountryCode(piece);
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

function getCountryCodeFromAcceptLanguage(req) {
  const rawHeader = req?.headers?.["accept-language"];
  const headerValue = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;
  const tokens = String(headerValue || "")
    .split(",")
    .map((part) => String(part || "").split(";")[0].trim())
    .filter(Boolean);

  for (const token of tokens) {
    const fromTag = extractCountryCodeFromLocaleTag(token);
    if (fromTag) {
      return fromTag;
    }
  }
  return "";
}

function getClientCountryCode(req) {
  const candidates = [
    req?.headers?.["cf-ipcountry"],
    req?.headers?.["x-vercel-ip-country"],
    req?.headers?.["cloudfront-viewer-country"],
    req?.headers?.["x-country-code"],
    req?.headers?.["x-country"],
    req?.headers?.["x-appengine-country"],
    req?.headers?.["x-geo-country"]
  ];
  for (const raw of candidates) {
    const first = Array.isArray(raw) ? raw[0] : raw;
    const token = String(first || "")
      .split(",")[0]
      .split(";")[0]
      .trim()
      .toUpperCase();
    const normalized = normalizeCountryCode(token);
    if (normalized) {
      return normalized;
    }
  }
  const acceptLanguageCountry = getCountryCodeFromAcceptLanguage(req);
  if (acceptLanguageCountry) {
    return acceptLanguageCountry;
  }
  return "";
}

function updateUserCountryFromRequest(username, req, { persistRegistered = false } = {}) {
  const normalizedUsername = normalizeUsername(username);
  if (!normalizedUsername) {
    return "";
  }
  const countryCode = getClientCountryCode(req);
  if (!countryCode) {
    return "";
  }

  if (users.has(normalizedUsername)) {
    const user = users.get(normalizedUsername);
    if (normalizeCountryCode(user?.countryCode) === countryCode) {
      return countryCode;
    }
    user.countryCode = countryCode;
    users.set(normalizedUsername, user);
    if (persistRegistered && !saveUsers()) {
      console.warn(`Failed to persist country code update for ${normalizedUsername}.`);
    }
    return countryCode;
  }

  if (guestUsers.has(normalizedUsername)) {
    const guest = guestUsers.get(normalizedUsername);
    if (normalizeCountryCode(guest?.countryCode) === countryCode) {
      return countryCode;
    }
    guest.countryCode = countryCode;
    guestUsers.set(normalizedUsername, guest);
    return countryCode;
  }

  return countryCode;
}

const I18N_AR_BY_EN = Object.freeze({
  "A supervisor cannot be kicked from this room.": "ظ„ط§ ظٹظ…ظƒظ† ط·ط±ط¯ ط§ظ„ظ…ط´ط±ظپ ظ…ظ† ظ‡ط°ظ‡ ط§ظ„ط؛ط±ظپط©.",
  "Account not found.": "ط§ظ„ط­ط³ط§ط¨ ط؛ظٹط± ظ…ظˆط¬ظˆط¯.",
  "Announcement is too long (max 500 chars).": "ط§ظ„ط±ط³ط§ظ„ط© ط§ظ„ط¹ط§ظ…ط© ط·ظˆظٹظ„ط© ط¬ط¯ظ‹ط§ (ط§ظ„ط­ط¯ ط§ظ„ط£ظ‚طµظ‰ 500 ط­ط±ظپ).",
  "Announcement text is required.": "ظ†طµ ط§ظ„ط±ط³ط§ظ„ط© ط§ظ„ط¹ط§ظ…ط© ظ…ط·ظ„ظˆط¨.",
  "Appeal reason must be 8-500 characters.": "ط³ط¨ط¨ ط·ظ„ط¨ ط±ظپط¹ ط§ظ„ط­ط¸ط± ظٹط¬ط¨ ط£ظ† ظٹظƒظˆظ† ط¨ظٹظ† 8 ظˆ500 ط­ط±ظپ.",
  "Avatar is invalid or too large.": "ط§ظ„طµظˆط±ط© ط§ظ„ط´ط®طµظٹط© ط؛ظٹط± طµط§ظ„ط­ط© ط£ظˆ ط­ط¬ظ…ظ‡ط§ ظƒط¨ظٹط± ط¬ط¯ظ‹ط§.",
  "Ban reason must be 3-300 characters.": "ط³ط¨ط¨ ط§ظ„ط­ط¸ط± ظٹط¬ط¨ ط£ظ† ظٹظƒظˆظ† ط¨ظٹظ† 3 ظˆ300 ط­ط±ظپ.",
  "Could not download this YouTube video right now. Try another one.": "طھط¹ط°ط± طھظ†ط²ظٹظ„ ظپظٹط¯ظٹظˆ ظٹظˆطھظٹظˆط¨ ط§ظ„ط¢ظ†. ط¬ط±ظ‘ط¨ ظپظٹط¯ظٹظˆ ط¢ط®ط±.",
  "Could not find a compatible playable format.": "طھط¹ط°ط± ط§ظ„ط¹ط«ظˆط± ط¹ظ„ظ‰ طµظٹط؛ط© طھط´ط؛ظٹظ„ ظ…طھظˆط§ظپظ‚ط©.",
  "Could not proxy YouTube video right now.": "طھط¹ط°ط± طھط´ط؛ظٹظ„ ظپظٹط¯ظٹظˆ ظٹظˆطھظٹظˆط¨ ط¹ط¨ط± ط§ظ„ط®ط§ط¯ظ… ط§ظ„ط¢ظ†.",
  "Could not reach YouTube right now. Try again.": "طھط¹ط°ط± ط§ظ„ط§طھطµط§ظ„ ط¨ظٹظˆطھظٹظˆط¨ ط§ظ„ط¢ظ†. ط­ط§ظˆظ„ ظ…ط±ط© ط£ط®ط±ظ‰.",
  "Display name must be 2 to 30 characters.": "ط§ظ„ط§ط³ظ… ط§ظ„ط¸ط§ظ‡ط± ظٹط¬ط¨ ط£ظ† ظٹظƒظˆظ† ظ…ظ† 2 ط¥ظ„ظ‰ 30 ط­ط±ظپظ‹ط§.",
  "Endpoint not found.": "ظ†ظ‚ط·ط© ط§ظ„ظˆطµظˆظ„ ط؛ظٹط± ظ…ظˆط¬ظˆط¯ط©.",
  "Enter a YouTube URL or search text.": "ط£ط¯ط®ظ„ ط±ط§ط¨ط· ظٹظˆطھظٹظˆط¨ ط£ظˆ ظ†طµ ط¨ط­ط«.",
  "Internal server error.": "ط­ط¯ط« ط®ط·ط£ ط¯ط§ط®ظ„ظٹ ظپظٹ ط§ظ„ط®ط§ط¯ظ….",
  "Invalid JSON body.": "ط¨ظٹط§ظ†ط§طھ JSON ط؛ظٹط± طµط§ظ„ط­ط©.",
  "Invalid action.": "ط§ظ„ط¥ط¬ط±ط§ط، ط؛ظٹط± طµط§ظ„ط­.",
  "Invalid login credentials.": "ط¨ظٹط§ظ†ط§طھ طھط³ط¬ظٹظ„ ط§ظ„ط¯ط®ظˆظ„ ط؛ظٹط± طµط­ظٹط­ط©.",
  "Invalid message identifier.": "ظ…ط¹ط±ظ‘ظپ ط§ظ„ط±ط³ط§ظ„ط© ط؛ظٹط± طµط§ظ„ط­.",
  "Invalid reaction payload.": "ط¨ظٹط§ظ†ط§طھ ط§ظ„طھظپط§ط¹ظ„ ط؛ظٹط± طµط§ظ„ط­ط©.",
  "Invalid reply identifier.": "ظ…ط¹ط±ظ‘ظپ ط§ظ„ط±ط¯ ط؛ظٹط± طµط§ظ„ط­.",
  "Invalid user identifier.": "ظ…ط¹ط±ظ‘ظپ ط§ظ„ظ…ط³طھط®ط¯ظ… ط؛ظٹط± طµط§ظ„ط­.",
  "Invalid video stream link.": "ط±ط§ط¨ط· ط¨ط« ط§ظ„ظپظٹط¯ظٹظˆ ط؛ظٹط± طµط§ظ„ط­.",
  "Invalid video sync action.": "ط¥ط¬ط±ط§ط، ظ…ط²ط§ظ…ظ†ط© ط§ظ„ظپظٹط¯ظٹظˆ ط؛ظٹط± طµط§ظ„ط­.",
  "Invalid video upload payload.": "ط¨ظٹط§ظ†ط§طھ ط±ظپط¹ ط§ظ„ظپظٹط¯ظٹظˆ ط؛ظٹط± طµط§ظ„ط­ط©.",
  "Join this room first.": "ط§ظ†ط¶ظ… ط¥ظ„ظ‰ ظ‡ط°ظ‡ ط§ظ„ط؛ط±ظپط© ط£ظˆظ„ظ‹ط§.",
  "Leader cannot kick themselves.": "ظ„ط§ ظٹظ…ظƒظ† ظ„ظ„ظ‚ط§ط¦ط¯ ط·ط±ط¯ ظ†ظپط³ظ‡.",
  "Message is empty.": "ط§ظ„ط±ط³ط§ظ„ط© ظپط§ط±ط؛ط©.",
  "Message is too long.": "ط§ظ„ط±ط³ط§ظ„ط© ط·ظˆظٹظ„ط© ط¬ط¯ظ‹ط§.",
  "Name must be 2-30 characters.": "ط§ظ„ط§ط³ظ… ظٹط¬ط¨ ط£ظ† ظٹظƒظˆظ† ط¨ظٹظ† 2 ظˆ30 ط­ط±ظپظ‹ط§.",
  "New Room": "ط؛ط±ظپط© ط¬ط¯ظٹط¯ط©",
  "No eligible message was found for reaction.": "ظ„ظ… ظٹطھظ… ط§ظ„ط¹ط«ظˆط± ط¹ظ„ظ‰ ط±ط³ط§ظ„ط© ظ…ظ†ط§ط³ط¨ط© ظ„ظ„طھظپط§ط¹ظ„.",
  "No join request found for this player.": "ظ„ط§ ظٹظˆط¬ط¯ ط·ظ„ط¨ ط§ظ†ط¶ظ…ط§ظ… ظ„ظ‡ط°ط§ ط§ظ„ظ„ط§ط¹ط¨.",
  "No matching YouTube video was found.": "ظ„ظ… ظٹطھظ… ط§ظ„ط¹ط«ظˆط± ط¹ظ„ظ‰ ظپظٹط¯ظٹظˆ ظٹظˆطھظٹظˆط¨ ظ…ط·ط§ط¨ظ‚.",
  "No room video is available.": "ظ„ط§ ظٹظˆط¬ط¯ ظپظٹط¯ظٹظˆ ظ…طھط§ط­ ظپظٹ ط§ظ„ط؛ط±ظپط©.",
  "No supported downloadable video format was found.": "ظ„ظ… ظٹطھظ… ط§ظ„ط¹ط«ظˆط± ط¹ظ„ظ‰ طµظٹط؛ط© ظپظٹط¯ظٹظˆ ظ‚ط§ط¨ظ„ط© ظ„ظ„طھظ†ط²ظٹظ„ ظˆظ…ط¯ط¹ظˆظ…ط©.",
  "Only the room leader can kick players.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط·ط±ط¯ ط§ظ„ظ„ط§ط¹ط¨ظٹظ†.",
  "Only the room leader can manage requests.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط¥ط¯ط§ط±ط© ط§ظ„ط·ظ„ط¨ط§طھ.",
  "Only the room leader can set video source.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ طھط­ط¯ظٹط¯ ظ…طµط¯ط± ط§ظ„ظپظٹط¯ظٹظˆ.",
  "Only the room leader can sync the video.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ظ…ط²ط§ظ…ظ†ط© ط§ظ„ظپظٹط¯ظٹظˆ.",
  "Only the room leader can upload videos.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط±ظپط¹ ط§ظ„ظپظٹط¯ظٹظˆ.",
  "Only the room leader can remove videos.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط­ط°ظپ ط§ظ„ظپظٹط¯ظٹظˆ.",
  "Only the room leader can view requests.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط¹ط±ط¶ ط§ظ„ط·ظ„ط¨ط§طھ.",
  "Only the room leader can transfer leadership.": "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ظ†ظ‚ظ„ ط§ظ„ظ‚ظٹط§ط¯ط©.",
  "Only a supervisor in this room can claim leadership.": "ظپظ‚ط· ظ…ط´ط±ظپ ظ…ظˆط¬ظˆط¯ ط¯ط§ط®ظ„ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط§ظ…طھظ„ط§ظƒ ط§ظ„ظ‚ظٹط§ط¯ط©.",
  "Payload is too large.": "ط­ط¬ظ… ط§ظ„ط¨ظٹط§ظ†ط§طھ ط§ظ„ظ…ط±ط³ظ„ط© ظƒط¨ظٹط± ط¬ط¯ظ‹ط§.",
  "Player is not in this room.": "ط§ظ„ظ„ط§ط¹ط¨ ظ„ظٹط³ ظپظٹ ظ‡ط°ظ‡ ط§ظ„ط؛ط±ظپط©.",
  "Please choose a valid video file.": "ظٹط±ط¬ظ‰ ط§ط®طھظٹط§ط± ظ…ظ„ظپ ظپظٹط¯ظٹظˆ طµط§ظ„ط­.",
  "Request not found.": "ط§ظ„ط·ظ„ط¨ ط؛ظٹط± ظ…ظˆط¬ظˆط¯.",
  "Room endpoint not found.": "ظ†ظ‚ط·ط© ظˆطµظˆظ„ ط§ظ„ط؛ط±ظپط© ط؛ظٹط± ظ…ظˆط¬ظˆط¯ط©.",
  "Room not found.": "ط§ظ„ط؛ط±ظپط© ط؛ظٹط± ظ…ظˆط¬ظˆط¯ط©.",
  "Server storage is unavailable right now.": "ظ…ط³ط§ط­ط© طھط®ط²ظٹظ† ط§ظ„ط®ط§ط¯ظ… ط؛ظٹط± ظ…طھط§ط­ط© ط­ط§ظ„ظٹظ‹ط§.",
  "Supervisor account cannot be banned.": "ظ„ط§ ظٹظ…ظƒظ† ط­ط¸ط± ط­ط³ط§ط¨ ط§ظ„ظ…ط´ط±ظپ.",
  "Supervisor account cannot be deleted.": "ظ„ط§ ظٹظ…ظƒظ† ط­ط°ظپ ط­ط³ط§ط¨ ط§ظ„ظ…ط´ط±ظپ.",
  "Target username is required.": "ط§ط³ظ… ط§ظ„ظ…ط³طھط®ط¯ظ… ط§ظ„ظ…ط³طھظ‡ط¯ظپ ظ…ط·ظ„ظˆط¨.",
  "Leader transfer target must be another member.": "ظٹط¬ط¨ ط§ط®طھظٹط§ط± ط¹ط¶ظˆ ط¢ط®ط± ط¯ط§ط®ظ„ ط§ظ„ط؛ط±ظپط© ظ„ظ†ظ‚ظ„ ط§ظ„ظ‚ظٹط§ط¯ط©.",
  "This account is not banned.": "ظ‡ط°ط§ ط§ظ„ط­ط³ط§ط¨ ط؛ظٹط± ظ…ط­ط¸ظˆط±.",
  "This action is supervisor-only.": "ظ‡ط°ط§ ط§ظ„ط¥ط¬ط±ط§ط، ظ…ط®طµطµ ظ„ظ„ظ…ط´ط±ظپ ظپظ‚ط·.",
  "This video version is outdated. Refresh room data.": "ط¥طµط¯ط§ط± ط§ظ„ظپظٹط¯ظٹظˆ ظ‡ط°ط§ ظ‚ط¯ظٹظ…. ط­ط¯ظ‘ط« ط¨ظٹط§ظ†ط§طھ ط§ظ„ط؛ط±ظپط©.",
  "Unauthorized.": "ط؛ظٹط± ظ…طµط±ط­.",
  "Unsupported video type. Use MP4, WebM or OGG.": "ظ†ظˆط¹ ط§ظ„ظپظٹط¯ظٹظˆ ط؛ظٹط± ظ…ط¯ط¹ظˆظ…. ط§ط³طھط®ط¯ظ… MP4 ط£ظˆ WebM ط£ظˆ OGG.",
  "User not found.": "ط§ظ„ظ…ط³طھط®ط¯ظ… ط؛ظٹط± ظ…ظˆط¬ظˆط¯.",
  "Username must be 3-30 characters and use only English letters and numbers.": "ط§ط³ظ… ط§ظ„ظ…ط³طھط®ط¯ظ… ظٹط¬ط¨ ط£ظ† ظٹظƒظˆظ† ظ…ظ† 3 ط¥ظ„ظ‰ 30 ط­ط±ظپظ‹ط§طŒ ظˆظٹط­طھظˆظٹ ظپظ‚ط· ط¹ظ„ظ‰ ط£ط­ط±ظپ ط¥ظ†ط¬ظ„ظٹط²ظٹط© ظˆط£ط±ظ‚ط§ظ….",
  "Username is already in use.": "ط§ط³ظ… ط§ظ„ظ…ط³طھط®ط¯ظ… ظ…ط³طھط®ط¯ظ… ط¨ط§ظ„ظپط¹ظ„.",
  "Video file is too large (max 1GB).": "ظ…ظ„ظپ ط§ظ„ظپظٹط¯ظٹظˆ ظƒط¨ظٹط± ط¬ط¯ظ‹ط§ (ط§ظ„ط­ط¯ ط§ظ„ط£ظ‚طµظ‰ 1GB).",
  "Video stream link expired. Please set the video again.": "ط§ظ†طھظ‡طھ طµظ„ط§ط­ظٹط© ط±ط§ط¨ط· ط¨ط« ط§ظ„ظپظٹط¯ظٹظˆ. ظٹط±ط¬ظ‰ طھط¹ظٹظٹظ† ط§ظ„ظپظٹط¯ظٹظˆ ظ…ط±ط© ط£ط®ط±ظ‰.",
  "You cannot join until the leader approves your request.": "ظ„ط§ ظٹظ…ظƒظ†ظƒ ط§ظ„ط§ظ†ط¶ظ…ط§ظ… ط­طھظ‰ ظٹظˆط§ظپظ‚ ط§ظ„ظ‚ط§ط¦ط¯ ط¹ظ„ظ‰ ط·ظ„ط¨ظƒ.",
  "YouTube video is too large (max 1GB).": "ظپظٹط¯ظٹظˆ ظٹظˆطھظٹظˆط¨ ظƒط¨ظٹط± ط¬ط¯ظ‹ط§ (ط§ظ„ط­ط¯ ط§ظ„ط£ظ‚طµظ‰ 1GB).",
  "Your join request is still pending.": "ط·ظ„ط¨ ط§ظ†ط¶ظ…ط§ظ…ظƒ ظ…ط§ ظٹط²ط§ظ„ ظ‚ظٹط¯ ط§ظ„ط§ظ†طھط¸ط§ط±."
});

const I18N_AR_BY_EN_REPAIRED = Object.freeze(
  Object.fromEntries(
    Object.entries(I18N_AR_BY_EN).map(([en, arText]) => [en, repairArabicMojibake(arText)])
  )
);

function i18n(req, ar, en) {
  if (getLang(req) === "en") {
    return en;
  }
  if (Object.prototype.hasOwnProperty.call(I18N_AR_BY_EN_REPAIRED, en)) {
    return I18N_AR_BY_EN_REPAIRED[en];
  }
  const repairedAr = repairArabicMojibake(ar);
  return looksLikeMojibake(repairedAr) ? en : repairedAr;
}

function securityHeaders(extra = {}) {
  return {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
    ...extra
  };
}

function sendJson(res, statusCode, payload) {
  res.writeHead(
    statusCode,
    securityHeaders({
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    })
  );
  res.end(JSON.stringify(payload));
}

function hashPasswordLegacy(password) {
  return crypto.createHash("sha256").update(password).digest("hex");
}

function hashPasswordSecure(password, salt) {
  return crypto.scryptSync(password, salt, 64).toString("hex");
}

function createPasswordRecord(password) {
  const passwordSalt = crypto.randomBytes(16).toString("hex");
  return {
    passwordSalt,
    passwordHash: hashPasswordSecure(password, passwordSalt)
  };
}

function verifyPassword(username, user, password) {
  const salt = String(user?.passwordSalt || "").trim();
  if (salt) {
    return user.passwordHash === hashPasswordSecure(password, salt);
  }
  if (user.passwordHash !== hashPasswordLegacy(password)) {
    return false;
  }
  // Upgrade legacy SHA-256 password hashes to salted scrypt on successful login.
  const next = createPasswordRecord(password);
  user.passwordSalt = next.passwordSalt;
  user.passwordHash = next.passwordHash;
  users.set(username, user);
  if (!saveUsers()) {
    console.warn("Password hash upgrade was not persisted for user:", username);
  }
  return true;
}

function normalizeUsername(value) {
  return String(value || "").trim().toLowerCase();
}

function sanitizeGuestDisplayName(value) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, 30);
}

function createGuestUsername(name) {
  const cleaned = String(name || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^\p{L}\p{N}_-]/gu, "")
    .slice(0, 20);
  const base = cleaned || "guest";
  let username = `${GUEST_USERNAME_PREFIX}${base}`;
  if (username.length < USERNAME_MIN_LENGTH) {
    username = `${GUEST_USERNAME_PREFIX}user`;
  }
  if (!users.has(username) && !guestUsers.has(username)) {
    return username.slice(0, USERNAME_MAX_LENGTH);
  }
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const suffix = crypto.randomBytes(2).toString("hex");
    const withSuffix = `${username.slice(0, Math.max(1, USERNAME_MAX_LENGTH - suffix.length - 1))}_${suffix}`;
    if (!users.has(withSuffix) && !guestUsers.has(withSuffix)) {
      return withSuffix;
    }
  }
  return `${GUEST_USERNAME_PREFIX}${crypto.randomBytes(5).toString("hex")}`.slice(0, USERNAME_MAX_LENGTH);
}

function getSessionMeta(entry) {
  if (typeof entry === "string") {
    return {
      username: normalizeUsername(entry),
      expiresAt: Date.now() + SESSION_TTL_MS,
      isGuest: false
    };
  }
  return {
    username: normalizeUsername(entry?.username),
    expiresAt: Number(entry?.expiresAt) || 0,
    isGuest: Boolean(entry?.isGuest)
  };
}

function hasActiveSessionForUser(username, exceptToken = "") {
  const target = normalizeUsername(username);
  if (!target) {
    return false;
  }
  let found = false;
  sessions.forEach((entry, token) => {
    if (token === exceptToken || found) {
      return;
    }
    const meta = getSessionMeta(entry);
    if (meta.username !== target) {
      return;
    }
    if (meta.expiresAt && meta.expiresAt < Date.now()) {
      return;
    }
    found = true;
  });
  return found;
}

function cleanupGuestUser(username) {
  const normalized = normalizeUsername(username);
  if (!guestUsers.has(normalized)) {
    return;
  }
  removeUserFromAllRooms(normalized);
  guestUsers.delete(normalized);
  userLastSeen.delete(normalized);
  scheduleRuntimeSnapshotPersistence();
}

function isValidUsername(username) {
  return (
    typeof username === "string" &&
    username.length >= USERNAME_MIN_LENGTH &&
    username.length <= USERNAME_MAX_LENGTH &&
    USERNAME_ALLOWED_REGEX.test(username)
  );
}

function isValidRegisteredUsername(username) {
  return (
    typeof username === "string" &&
    username.length >= USERNAME_MIN_LENGTH &&
    username.length <= USERNAME_MAX_LENGTH &&
    REGISTERED_USERNAME_ALLOWED_REGEX.test(username)
  );
}

function isValidPassword(password) {
  return (
    typeof password === "string" &&
    password.length >= PASSWORD_MIN_LENGTH &&
    password.length <= PASSWORD_MAX_LENGTH
  );
}

function isSupervisor(username) {
  return String(username || "").trim().toLowerCase() === SUPERVISOR_USERNAME;
}

function getBanRecord(username) {
  const key = String(username || "").trim().toLowerCase();
  if (!key) {
    return null;
  }
  return bannedUsers.get(key) || null;
}

function isBanned(username) {
  return Boolean(getBanRecord(username));
}

function formatBanRecord(entry) {
  if (!entry) {
    return null;
  }
  return {
    username: entry.username,
    reason: entry.reason,
    bannedBy: entry.bannedBy,
    bannedAt: entry.bannedAt
  };
}

function formatUnbanRequest(entry) {
  if (!entry) {
    return null;
  }
  return {
    username: entry.username,
    reason: entry.reason,
    status: entry.status,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
    reviewedBy: entry.reviewedBy,
    reviewNote: entry.reviewNote
  };
}

function formatSiteAnnouncement(entry) {
  if (!entry) {
    return null;
  }
  const cleanText = String(entry.text || "").trim();
  if (!cleanText || looksLikeMojibake(cleanText)) {
    return null;
  }
  const createdAt = Number(entry.createdAt) || Date.now();
  const rawId = String(entry.id || "").trim();
  return {
    id: rawId || `legacy-${createdAt}`,
    text: cleanText.slice(0, 500),
    createdBy: String(entry.createdBy || SUPERVISOR_USERNAME).trim().toLowerCase() || SUPERVISOR_USERNAME,
    createdAt
  };
}

function getLiveSiteAnnouncement() {
  const announcement = formatSiteAnnouncement(siteAnnouncement);
  if (!announcement) {
    return null;
  }
  const age = Date.now() - Number(announcement.createdAt || 0);
  if (age < 0 || age > SITE_ANNOUNCEMENT_LIVE_MS) {
    return null;
  }
  return announcement;
}

function sendBannedResponse(req, res, username) {
  const ban = formatBanRecord(getBanRecord(username));
  sendJson(res, 403, {
    code: "ACCOUNT_BANNED",
    error: i18n(
      req,
      "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½. ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.",
      "This account is banned from the site. You can send an unban request to the supervisor."
    ),
    ban,
    unbanRequest: formatUnbanRequest(unbanRequests.get(String(username || "").toLowerCase()) || null),
    announcement: getLiveSiteAnnouncement()
  });
}

function assertSupervisor(req, res, username) {
  if (isSupervisor(username)) {
    return true;
  }
  sendJson(res, 403, {
    code: "SUPERVISOR_ONLY",
    error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "This action is supervisor-only.")
  });
  return false;
}

function assertNotBanned(req, res, username) {
  if (!username) {
    return false;
  }
  if (isSupervisor(username)) {
    return true;
  }
  if (!isBanned(username)) {
    return true;
  }
  sendBannedResponse(req, res, username);
  return false;
}

function isUserOnline(username) {
  const lastSeen = Number(userLastSeen.get(username) || 0);
  return Date.now() - lastSeen <= 15000;
}

function touchUser(username) {
  if (!username) {
    return;
  }
  userLastSeen.set(username, Date.now());
}

function publicProfile(username) {
  const info = users.get(username) || guestUsers.get(username);
  if (!info) {
    return null;
  }
  return {
    username,
    displayName: normalizeDisplayName(info.displayName, username),
    avatarDataUrl: info.avatarDataUrl || "",
    countryCode: normalizeCountryCode(info.countryCode),
    createdAt: info.createdAt,
    isOnline: isUserOnline(username),
    isSupervisor: isSupervisor(username),
    isGuest: guestUsers.has(username)
  };
}

function createSession(username, options = {}) {
  const token = randomToken();
  const normalizedUsername = normalizeUsername(username);
  const isGuest = Boolean(options.isGuest);
  sessions.set(token, {
    username: normalizedUsername,
    expiresAt: Date.now() + SESSION_TTL_MS,
    isGuest
  });
  touchUser(normalizedUsername);
  scheduleRuntimeSnapshotPersistence();
  return token;
}

function getSessionUsername(token) {
  if (!token || !sessions.has(token)) {
    return null;
  }
  const normalized = getSessionMeta(sessions.get(token));
  const exists = normalized.isGuest
    ? guestUsers.has(normalized.username)
    : users.has(normalized.username);
  if (!normalized.username || !exists) {
    sessions.delete(token);
    scheduleRuntimeSnapshotPersistence();
    return null;
  }
  if (normalized.expiresAt && normalized.expiresAt < Date.now()) {
    sessions.delete(token);
    if (normalized.isGuest && !hasActiveSessionForUser(normalized.username, token)) {
      cleanupGuestUser(normalized.username);
    }
    scheduleRuntimeSnapshotPersistence();
    return null;
  }

  normalized.expiresAt = Date.now() + SESSION_TTL_MS;
  sessions.set(token, normalized);
  return normalized.username;
}

function removeUserSessions(username) {
  let touched = false;
  sessions.forEach((entry, token) => {
    const sessionUser = getSessionMeta(entry).username;
    if (sessionUser === username) {
      sessions.delete(token);
      touched = true;
    }
  });
  if (touched) {
    scheduleRuntimeSnapshotPersistence();
  }
}

function removeUserFromAllRooms(username) {
  let touched = false;
  rooms.forEach((room) => {
    let roomRef = room;
    if (roomRef.members.has(username)) {
      const result = removeMemberFromRoom(roomRef, username);
      if (result.deleted || !result.room) {
        return;
      }
      roomRef = result.room;
      touched = true;
    }

    if (roomRef.joinRequests.has(username)) {
      roomRef.joinRequests.delete(username);
      touched = true;
    }
    if (roomRef.approvedUsers instanceof Set) {
      roomRef.approvedUsers.delete(username);
      touched = true;
    }
    if (roomRef.messageFloorByUser instanceof Map) {
      roomRef.messageFloorByUser.delete(username);
      touched = true;
    }
    if (roomRef.pendingHostRestore === username) {
      roomRef.pendingHostRestore = null;
      touched = true;
    }
  });
  if (touched) {
    scheduleRuntimeSnapshotPersistence();
  }
}

function countOnlineUsers() {
  let count = 0;
  users.forEach((_, username) => {
    if (isUserOnline(username)) {
      count += 1;
    }
  });
  guestUsers.forEach((_, username) => {
    if (isUserOnline(username)) {
      count += 1;
    }
  });
  return count;
}

function countUsersInsideRooms() {
  const inside = new Set();
  rooms.forEach((room) => {
    room.members.forEach((username) => inside.add(username));
  });
  return inside.size;
}

function formatAdminUserSummary(username) {
  const info = users.get(username);
  if (!info) {
    return null;
  }
  const isOnline = isUserOnline(username);
  return {
    username,
    displayName: normalizeDisplayName(info.displayName, username),
    createdAt: Number(info.createdAt) || 0,
    isOnline,
    isBanned: isBanned(username),
    ban: formatBanRecord(getBanRecord(username)),
    isSupervisor: isSupervisor(username)
  };
}

function randomToken() {
  return crypto.randomBytes(24).toString("hex");
}

function randomRoomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 6; i += 1) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

function normalizeRoomVideoDuration(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 0;
  }
  return Math.min(numeric, ROOM_VIDEO_MAX_DURATION_SEC);
}

function normalizeRoomVideoPlaybackRate(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 1;
  }
  return Math.min(3, Math.max(0.25, numeric));
}

function clampRoomVideoTime(room, value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return 0;
  }
  const duration = normalizeRoomVideoDuration(room?.video?.duration);
  if (duration > 0) {
    return Math.min(duration, numeric);
  }
  return numeric;
}

function parseBooleanFlag(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value > 0;
  }
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function shouldEnqueueVideo(value, mode = "") {
  const normalizedMode = String(mode || "").trim().toLowerCase();
  if (normalizedMode === "queue" || normalizedMode === "enqueue") {
    return true;
  }
  if (normalizedMode === "replace" || normalizedMode === "play-now" || normalizedMode === "now") {
    return false;
  }
  return parseBooleanFlag(value);
}

function resolveRoomVideoEnqueueMode(room, username, enqueueValue, modeValue = "") {
  const hasActiveVideo = Boolean(room?.video?.src);
  if (!hasActiveVideo) {
    // Keep the very first item as current video instead of a silent queue-only state.
    return false;
  }
  const requestedEnqueue = shouldEnqueueVideo(enqueueValue, modeValue);
  if (requestedEnqueue) {
    return true;
  }
  // Non-leaders can add items but cannot replace current playback.
  return room?.host !== normalizeUsername(username);
}

function roomVideoPublicToAbsolutePath(publicPath) {
  const storedFileName = getStoredFileNameFromPublicSrc(publicPath);
  if (!storedFileName) {
    return null;
  }
  const absolute = path.join(getRoomVideoUploadDir(), storedFileName);
  const normalizedRoot = path.resolve(getRoomVideoUploadDir());
  const normalizedAbs = path.resolve(absolute);
  if (!normalizedAbs.startsWith(normalizedRoot + path.sep) && normalizedAbs !== normalizedRoot) {
    return null;
  }
  return absolute;
}

function buildRoomVideoBlobSrc(blobId) {
  const cleanId = String(blobId || "").trim();
  if (!cleanId) {
    return "";
  }
  return `${ROOM_VIDEO_BLOB_API_PREFIX}${cleanId}`;
}

function extractRoomVideoBlobIdFromSrc(src) {
  const clean = String(src || "").trim();
  if (!clean.startsWith(ROOM_VIDEO_BLOB_API_PREFIX)) {
    return "";
  }
  const value = clean.slice(ROOM_VIDEO_BLOB_API_PREFIX.length).split(/[?#]/)[0] || "";
  return /^[A-Za-z0-9_-]+$/.test(value) ? value : "";
}

function getRoomVideoBlobId(entry) {
  if (!entry) {
    return "";
  }
  const explicit = String(entry.blobId || "").trim();
  if (explicit) {
    return explicit;
  }
  return extractRoomVideoBlobIdFromSrc(entry.src);
}

function normalizeSqlVideoBlobChunk(value) {
  if (!value) {
    return Buffer.alloc(0);
  }
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) {
    return Buffer.from(value);
  }
  if (typeof value === "string") {
    return Buffer.from(value, "binary");
  }
  return Buffer.alloc(0);
}

function isRoomVideoBlobReferenced(blobId) {
  const cleanId = String(blobId || "").trim();
  if (!cleanId) {
    return false;
  }
  for (const room of rooms.values()) {
    const currentBlobId = getRoomVideoBlobId(room?.video || null);
    if (currentBlobId === cleanId) {
      return true;
    }
    const queue = Array.isArray(room?.videoQueue) ? room.videoQueue : [];
    for (const entry of queue) {
      if (getRoomVideoBlobId(entry) === cleanId) {
        return true;
      }
    }
    const history = Array.isArray(room?.videoHistory) ? room.videoHistory : [];
    for (const entry of history) {
      if (getRoomVideoBlobId(entry) === cleanId) {
        return true;
      }
    }
  }
  return false;
}

function readRoomVideoBlobBufferFromDisk(filePath) {
  const target = String(filePath || "").trim();
  if (!target) {
    return null;
  }
  try {
    if (!fs.existsSync(target)) {
      return null;
    }
    const payload = fs.readFileSync(target);
    if (!Buffer.isBuffer(payload) || payload.length <= 0) {
      return null;
    }
    if (payload.length > ROOM_VIDEO_MAX_BYTES) {
      return null;
    }
    return payload;
  } catch (_error) {
    return null;
  }
}

async function persistRoomVideoBlobBufferToSql(payload, {
  mimeType = "video/mp4",
  sourceKind = "upload",
  fileName = "",
  youtubeId = ""
} = {}) {
  if (!isSqlRuntimeEnabled()) {
    return null;
  }
  const binary = normalizeSqlVideoBlobChunk(payload);
  if (!binary.length || binary.length > ROOM_VIDEO_MAX_BYTES) {
    return null;
  }
  const blobId = randomToken();
  const createdAt = Date.now();
  const cleanMimeType = normalizeVideoMimeType(mimeType) || "video/mp4";
  const cleanSourceKind = String(sourceKind || "upload").trim().slice(0, 32) || "upload";
  const cleanFileName = sanitizeUploadedFilename(fileName || "video");
  const cleanYouTubeId = String(youtubeId || "").trim().slice(0, 40);
  if (pgRuntimeEnabled && pgClient) {
    await pgClient.query(
      "INSERT INTO app_video_assets (id, mime_type, byte_size, payload, created_at, source_kind, file_name, youtube_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
      [
        blobId,
        cleanMimeType,
        Number(binary.length),
        binary,
        Number(createdAt),
        cleanSourceKind,
        cleanFileName,
        cleanYouTubeId
      ]
    );
    return {
      blobId,
      src: buildRoomVideoBlobSrc(blobId),
      size: Number(binary.length),
      mimeType: cleanMimeType
    };
  }
  if (mysqlRuntimeEnabled && mysqlPool) {
    await mysqlPool.execute(
      "INSERT INTO app_video_assets (id, mime_type, byte_size, payload, created_at, source_kind, file_name, youtube_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [
        blobId,
        cleanMimeType,
        Number(binary.length),
        binary,
        Number(createdAt),
        cleanSourceKind,
        cleanFileName,
        cleanYouTubeId
      ]
    );
    return {
      blobId,
      src: buildRoomVideoBlobSrc(blobId),
      size: Number(binary.length),
      mimeType: cleanMimeType
    };
  }
  return null;
}

async function persistRoomVideoBlobFromFileToSql(filePath, meta = {}) {
  const payload = readRoomVideoBlobBufferFromDisk(filePath);
  if (!payload) {
    return null;
  }
  return persistRoomVideoBlobBufferToSql(payload, meta);
}

async function deleteRoomVideoBlobFromSql(blobId) {
  const cleanId = String(blobId || "").trim();
  if (!cleanId || !isSqlRuntimeEnabled()) {
    return;
  }
  if (pgRuntimeEnabled && pgClient) {
    await pgClient.query("DELETE FROM app_video_assets WHERE id = $1", [cleanId]);
    return;
  }
  if (mysqlRuntimeEnabled && mysqlPool) {
    await mysqlPool.execute("DELETE FROM app_video_assets WHERE id = ?", [cleanId]);
  }
}

async function fetchRoomVideoBlobMetaFromSql(blobId) {
  const cleanId = String(blobId || "").trim();
  if (!cleanId || !isSqlRuntimeEnabled()) {
    return null;
  }
  if (pgRuntimeEnabled && pgClient) {
    const result = await pgClient.query(
      "SELECT id, mime_type, byte_size, created_at FROM app_video_assets WHERE id = $1 LIMIT 1",
      [cleanId]
    );
    const row = result.rows[0];
    if (!row) {
      return null;
    }
    return {
      id: String(row.id || cleanId),
      mimeType: normalizeVideoMimeType(row.mime_type) || "video/mp4",
      size: Math.max(0, Number(row.byte_size || 0)),
      createdAt: Number(row.created_at || 0)
    };
  }
  if (mysqlRuntimeEnabled && mysqlPool) {
    const [rows] = await mysqlPool.query(
      "SELECT id, mime_type, byte_size, created_at FROM app_video_assets WHERE id = ? LIMIT 1",
      [cleanId]
    );
    const row = Array.isArray(rows) ? rows[0] : null;
    if (!row) {
      return null;
    }
    return {
      id: String(row.id || cleanId),
      mimeType: normalizeVideoMimeType(row.mime_type) || "video/mp4",
      size: Math.max(0, Number(row.byte_size || 0)),
      createdAt: Number(row.created_at || 0)
    };
  }
  return null;
}

async function fetchLatestYouTubeRoomVideoBlobFromSql(youtubeId) {
  const cleanYouTubeId = String(youtubeId || "").trim();
  if (!cleanYouTubeId || !isSqlRuntimeEnabled()) {
    return null;
  }
  if (pgRuntimeEnabled && pgClient) {
    const result = await pgClient.query(
      "SELECT id, mime_type, byte_size, file_name, created_at FROM app_video_assets WHERE youtube_id = $1 AND source_kind = 'youtube' ORDER BY created_at DESC LIMIT 1",
      [cleanYouTubeId]
    );
    const row = result.rows[0];
    if (!row) {
      return null;
    }
    const blobId = String(row.id || "").trim();
    if (!blobId) {
      return null;
    }
    return {
      blobId,
      src: buildRoomVideoBlobSrc(blobId),
      mimeType: normalizeVideoMimeType(row.mime_type) || "video/mp4",
      size: Math.max(0, Number(row.byte_size || 0)),
      fileName: sanitizeUploadedFilename(row.file_name || `youtube-${cleanYouTubeId}`),
      createdAt: Number(row.created_at || 0)
    };
  }
  if (mysqlRuntimeEnabled && mysqlPool) {
    const [rows] = await mysqlPool.query(
      "SELECT id, mime_type, byte_size, file_name, created_at FROM app_video_assets WHERE youtube_id = ? AND source_kind = 'youtube' ORDER BY created_at DESC LIMIT 1",
      [cleanYouTubeId]
    );
    const row = Array.isArray(rows) ? rows[0] : null;
    if (!row) {
      return null;
    }
    const blobId = String(row.id || "").trim();
    if (!blobId) {
      return null;
    }
    return {
      blobId,
      src: buildRoomVideoBlobSrc(blobId),
      mimeType: normalizeVideoMimeType(row.mime_type) || "video/mp4",
      size: Math.max(0, Number(row.byte_size || 0)),
      fileName: sanitizeUploadedFilename(row.file_name || `youtube-${cleanYouTubeId}`),
      createdAt: Number(row.created_at || 0)
    };
  }
  return null;
}

async function fetchRoomVideoBlobChunkFromSql(blobId, offset, length) {
  const cleanId = String(blobId || "").trim();
  const startOffset = Math.max(0, Number(offset) || 0);
  const take = Math.max(0, Number(length) || 0);
  if (!cleanId || !take || !isSqlRuntimeEnabled()) {
    return Buffer.alloc(0);
  }
  if (pgRuntimeEnabled && pgClient) {
    const result = await pgClient.query(
      "SELECT substring(payload from $2 for $3) AS chunk FROM app_video_assets WHERE id = $1 LIMIT 1",
      [cleanId, startOffset + 1, take]
    );
    const row = result.rows[0];
    return normalizeSqlVideoBlobChunk(row?.chunk);
  }
  if (mysqlRuntimeEnabled && mysqlPool) {
    const [rows] = await mysqlPool.query(
      "SELECT SUBSTRING(payload, ?, ?) AS chunk FROM app_video_assets WHERE id = ? LIMIT 1",
      [startOffset + 1, take, cleanId]
    );
    const row = Array.isArray(rows) ? rows[0] : null;
    return normalizeSqlVideoBlobChunk(row?.chunk);
  }
  return Buffer.alloc(0);
}

async function serveRoomVideoBlobFromSql(req, res, blobId) {
  const method = req.method === "HEAD" ? "HEAD" : req.method;
  if (method !== "GET" && method !== "HEAD") {
    res.writeHead(
      405,
      securityHeaders({
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
        Allow: "GET, HEAD"
      })
    );
    res.end(JSON.stringify({ error: "Method not allowed." }));
    return;
  }

  const meta = await fetchRoomVideoBlobMetaFromSql(blobId);
  if (!meta || !meta.size) {
    res.writeHead(
      404,
      securityHeaders({
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store"
      })
    );
    res.end(JSON.stringify({ error: "No room video is available." }));
    return;
  }

  const totalSize = Math.max(0, Number(meta.size || 0));
  const mimeType = normalizeVideoMimeType(meta.mimeType) || "video/mp4";
  const rangeHeader = String(req.headers.range || "").trim();
  let start = 0;
  let end = totalSize - 1;
  let statusCode = 200;

  if (rangeHeader) {
    const match = rangeHeader.match(/^bytes=(\d*)-(\d*)$/i);
    if (!match) {
      res.writeHead(
        416,
        securityHeaders({
          "Content-Range": `bytes */${totalSize}`,
          "Cache-Control": "no-store"
        })
      );
      res.end();
      return;
    }
    if (match[1] === "" && match[2] === "") {
      res.writeHead(
        416,
        securityHeaders({
          "Content-Range": `bytes */${totalSize}`,
          "Cache-Control": "no-store"
        })
      );
      res.end();
      return;
    }
    if (match[1] === "") {
      const suffixLength = Number(match[2]);
      if (!Number.isInteger(suffixLength) || suffixLength <= 0) {
        res.writeHead(
          416,
          securityHeaders({
            "Content-Range": `bytes */${totalSize}`,
            "Cache-Control": "no-store"
          })
        );
        res.end();
        return;
      }
      start = Math.max(0, totalSize - suffixLength);
      end = totalSize - 1;
    } else {
      start = Number(match[1]);
      end = match[2] === "" ? totalSize - 1 : Number(match[2]);
    }
    if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end < start || start >= totalSize) {
      res.writeHead(
        416,
        securityHeaders({
          "Content-Range": `bytes */${totalSize}`,
          "Cache-Control": "no-store"
        })
      );
      res.end();
      return;
    }
    end = Math.min(totalSize - 1, end);
    statusCode = 206;
  }

  const contentLength = end - start + 1;
  const headers = {
    "Content-Type": mimeType,
    "Cache-Control": "public, max-age=120, must-revalidate",
    "Accept-Ranges": "bytes",
    "Content-Length": contentLength
  };
  if (statusCode === 206) {
    headers["Content-Range"] = `bytes ${start}-${end}/${totalSize}`;
  }
  res.writeHead(statusCode, securityHeaders(headers));
  if (method === "HEAD") {
    res.end();
    return;
  }

  let cursor = start;
  let remaining = contentLength;
  let aborted = false;
  req.on("close", () => {
    aborted = true;
  });
  while (remaining > 0 && !aborted && !res.writableEnded) {
    const take = Math.min(ROOM_VIDEO_DB_STREAM_CHUNK_BYTES, remaining);
    const chunk = await fetchRoomVideoBlobChunkFromSql(blobId, cursor, take);
    if (!chunk || !chunk.length) {
      break;
    }
    if (!res.write(chunk)) {
      await new Promise((resolve) => res.once("drain", resolve));
    }
    cursor += chunk.length;
    remaining -= chunk.length;
  }
  res.end();
}

function collectReferencedRoomVideoBlobIds() {
  const ids = new Set();
  rooms.forEach((room) => {
    const currentBlobId = getRoomVideoBlobId(room?.video || null);
    if (currentBlobId) {
      ids.add(currentBlobId);
    }
    const queue = Array.isArray(room?.videoQueue) ? room.videoQueue : [];
    queue.forEach((entry) => {
      const id = getRoomVideoBlobId(entry);
      if (id) {
        ids.add(id);
      }
    });
    const history = Array.isArray(room?.videoHistory) ? room.videoHistory : [];
    history.forEach((entry) => {
      const id = getRoomVideoBlobId(entry);
      if (id) {
        ids.add(id);
      }
    });
  });
  return ids;
}

async function pruneOrphanedRoomVideoBlobsFromSql() {
  if (!isSqlRuntimeEnabled()) {
    return;
  }
  const referenced = collectReferencedRoomVideoBlobIds();
  try {
    if (pgRuntimeEnabled && pgClient) {
      const result = await pgClient.query("SELECT id FROM app_video_assets");
      for (const row of result.rows) {
        const id = String(row?.id || "").trim();
        if (!id || referenced.has(id)) {
          continue;
        }
        await pgClient.query("DELETE FROM app_video_assets WHERE id = $1", [id]);
      }
      return;
    }
    if (mysqlRuntimeEnabled && mysqlPool) {
      const [rows] = await mysqlPool.query("SELECT id FROM app_video_assets");
      for (const row of rows || []) {
        const id = String(row?.id || "").trim();
        if (!id || referenced.has(id)) {
          continue;
        }
        await mysqlPool.execute("DELETE FROM app_video_assets WHERE id = ?", [id]);
      }
    }
  } catch (error) {
    console.error("Failed to prune orphaned room video blobs:", error.message);
  }
}

function createRoomVideoSyncState(videoId) {
  return {
    videoId: String(videoId || randomToken()),
    playing: true,
    baseTime: 0,
    playbackRate: 1,
    updatedAt: Date.now(),
    clientNow: 0,
    clientSeq: 0
  };
}

function cloneRoomVideoEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }
  const blobId = String(entry.blobId || extractRoomVideoBlobIdFromSrc(entry.src)).trim();
  return {
    id: String(entry.id || randomToken()),
    sourceType: String(entry.sourceType || "file"),
    youtubeId: String(entry.youtubeId || ""),
    src: String(entry.src || ""),
    blobId,
    filename: String(entry.filename || "video"),
    mimeType: String(entry.mimeType || "video/mp4"),
    size: Number(entry.size || 0),
    uploadedBy: String(entry.uploadedBy || ""),
    uploadedAt: Number(entry.uploadedAt || Date.now()),
    duration: normalizeRoomVideoDuration(entry.duration),
    isYouTubeCached: Boolean(entry.isYouTubeCached),
    isYouTubeProxy: Boolean(entry.isYouTubeProxy)
  };
}

function formatRoomVideoEntry(entry, room, syncState = null) {
  if (!entry) {
    return null;
  }
  const currentTime = syncState ? getRoomVideoEffectiveTime({ video: entry, videoSync: syncState }) : 0;
  return {
    id: String(entry.id || ""),
    sourceType: String(entry.sourceType || "file"),
    youtubeId: String(entry.youtubeId || ""),
    src: String(entry.src || ""),
    filename: String(entry.filename || "video"),
    mimeType: String(entry.mimeType || "video/mp4"),
    size: Number(entry.size || 0),
    uploadedBy: String(entry.uploadedBy || room?.host || ""),
    uploadedAt: Number(entry.uploadedAt || room?.createdAt || Date.now()),
    duration: normalizeRoomVideoDuration(entry.duration),
    sync: syncState ? {
      videoId: String(syncState.videoId || ""),
      playing: Boolean(syncState.playing),
      currentTime,
      playbackRate: normalizeRoomVideoPlaybackRate(syncState.playbackRate),
      updatedAt: Number(syncState.updatedAt || Date.now()),
      clientNow: Number(syncState.clientNow || 0),
      clientSeq: Number(syncState.clientSeq || 0)
    } : null
  };
}

function cleanupRoomVideoEntryAsset(videoEntry) {
  if (!videoEntry || !videoEntry.src) {
    return;
  }
  const blobId = getRoomVideoBlobId(videoEntry);
  if (blobId) {
    deleteRoomVideoBlobFromSql(blobId).catch((error) => {
      console.error("Failed to delete room video blob asset:", error.message);
    });
  }
  const shouldPreserveCachedFile = Boolean(videoEntry.isYouTubeCached || isVideoSrcInYouTubeCache(videoEntry.src));
  const absolutePath = shouldPreserveCachedFile ? null : roomVideoPublicToAbsolutePath(videoEntry.src);
  if (absolutePath && fs.existsSync(absolutePath)) {
    try {
      fs.unlinkSync(absolutePath);
    } catch (_error) {
      // Ignore cleanup errors so room operations continue.
    }
  }
  const proxyId = extractYouTubeProxyIdFromSrc(videoEntry.src);
  if (proxyId) {
    youtubeProxyStreams.delete(proxyId);
  }
}

function trimRoomVideoCollections(room) {
  normalizeRoomVideoCollections(room);
  // Played videos are not retained: they are cleaned immediately.
  const MAX_HISTORY = 0;
  const MAX_QUEUE = 80;
  if (room.videoHistory.length > MAX_HISTORY) {
    const overflow = room.videoHistory.splice(0, room.videoHistory.length - MAX_HISTORY);
    overflow.forEach((entry) => cleanupRoomVideoEntryAsset(entry));
  }
  if (room.videoQueue.length > MAX_QUEUE) {
    const overflow = room.videoQueue.splice(MAX_QUEUE);
    overflow.forEach((entry) => cleanupRoomVideoEntryAsset(entry));
  }
}

function setRoomCurrentVideo(room, videoEntry, options = {}) {
  if (!room) {
    return null;
  }
  normalizeRoomVideoCollections(room);
  const preserveCurrentInHistory = options.preserveCurrentInHistory !== false;
  const nextEntry = cloneRoomVideoEntry(videoEntry);
  if (!nextEntry) {
    return null;
  }
  if (room.video) {
    if (preserveCurrentInHistory) {
      room.videoHistory.push(cloneRoomVideoEntry(room.video));
    } else {
      cleanupRoomVideoEntryAsset(room.video);
    }
  }
  room.video = nextEntry;
  room.videoSync = createRoomVideoSyncState(nextEntry.id);
  trimRoomVideoCollections(room);
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_set");
  return room.video;
}

function queueRoomVideoEntry(room, videoEntry) {
  if (!room || !videoEntry) {
    return null;
  }
  normalizeRoomVideoCollections(room);
  const entry = cloneRoomVideoEntry(videoEntry);
  if (!entry) {
    return null;
  }
  room.videoQueue.push(entry);
  trimRoomVideoCollections(room);
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_queue_updated");
  return entry;
}

function playNextQueuedRoomVideo(room) {
  if (!room) {
    return null;
  }
  normalizeRoomVideoCollections(room);
  if (room.videoQueue.length === 0) {
    return null;
  }
  const next = room.videoQueue.shift();
  if (room.video) {
    room.videoHistory.push(cloneRoomVideoEntry(room.video));
  }
  room.video = cloneRoomVideoEntry(next);
  room.videoSync = createRoomVideoSyncState(room.video.id);
  // Keep next item anchored at zero until leader emits an explicit play sync.
  room.videoSync.playing = false;
  room.videoSync.baseTime = 0;
  room.videoSync.updatedAt = Date.now();
  trimRoomVideoCollections(room);
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_queue_next");
  return room.video;
}

function playPreviousRoomVideo(room) {
  if (!room) {
    return null;
  }
  normalizeRoomVideoCollections(room);
  if (room.videoHistory.length === 0) {
    return null;
  }
  const previous = room.videoHistory.pop();
  if (room.video) {
    room.videoQueue.unshift(cloneRoomVideoEntry(room.video));
  }
  room.video = cloneRoomVideoEntry(previous);
  room.videoSync = createRoomVideoSyncState(room.video.id);
  trimRoomVideoCollections(room);
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_queue_prev");
  return room.video;
}

function removeQueuedRoomVideoAt(room, index) {
  if (!room) {
    return null;
  }
  normalizeRoomVideoCollections(room);
  if (!Number.isInteger(index) || index < 0 || index >= room.videoQueue.length) {
    return null;
  }
  const [removed] = room.videoQueue.splice(index, 1);
  cleanupRoomVideoEntryAsset(removed);
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_queue_updated");
  return removed || null;
}

function clearRoomVideoQueue(room, { clearHistory = false } = {}) {
  if (!room) {
    return;
  }
  normalizeRoomVideoCollections(room);
  room.videoQueue.forEach((entry) => cleanupRoomVideoEntryAsset(entry));
  room.videoQueue = [];
  if (clearHistory) {
    room.videoHistory.forEach((entry) => cleanupRoomVideoEntryAsset(entry));
    room.videoHistory = [];
  }
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_queue_updated");
}

function removeRoomVideoAsset(room, options = {}) {
  if (!room) {
    return;
  }
  const clearQueue = Boolean(options.clearQueue);
  const clearHistory = Boolean(options.clearHistory);
  if (room.video) {
    cleanupRoomVideoEntryAsset(room.video);
  }
  room.video = null;
  room.videoSync = null;
  if (clearQueue || clearHistory) {
    clearRoomVideoQueue(room, { clearHistory });
  }
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "video_removed");
}

function createRoomFileVideoEntry(room, {
  src,
  storedFileName,
  filename,
  mimeType,
  size,
  uploadedBy,
  duration = 0,
  youtubeId = ""
}) {
  if (!room?.code) {
    return null;
  }
  let cleanSrc = String(src || "").trim();
  if (!cleanSrc) {
    const cleanStoredFileName = String(storedFileName || "").trim();
    if (!cleanStoredFileName) {
      return null;
    }
    cleanSrc = `${ROOM_VIDEO_PUBLIC_PREFIX}${cleanStoredFileName}`;
  }
  const blobId = extractRoomVideoBlobIdFromSrc(cleanSrc);
  return {
    id: randomToken(),
    sourceType: "file",
    youtubeId: String(youtubeId || "").trim(),
    src: cleanSrc,
    blobId,
    filename: sanitizeUploadedFilename(filename || blobId || storedFileName || "video"),
    mimeType: String(mimeType || "video/mp4"),
    size: Number(size || 0),
    uploadedBy: normalizeUsername(uploadedBy),
    uploadedAt: Date.now(),
    duration: normalizeRoomVideoDuration(duration),
    isYouTubeCached: false,
    isYouTubeProxy: false
  };
}

function createYouTubeEmbedVideoEntry(videoId, uploadedBy, label = "", duration = 0) {
  const cleanVideoId = String(videoId || "").trim();
  if (!cleanVideoId) {
    return null;
  }
  return {
    id: randomToken(),
    sourceType: "youtube",
    youtubeId: cleanVideoId,
    src: `https://www.youtube.com/watch?v=${cleanVideoId}`,
    filename: sanitizeUploadedFilename(label || `youtube-${cleanVideoId}`),
    mimeType: "video/youtube",
    size: 0,
    uploadedBy: normalizeUsername(uploadedBy),
    uploadedAt: Date.now(),
    duration: normalizeRoomVideoDuration(duration),
    isYouTubeCached: false,
    isYouTubeProxy: false
  };
}

function setOrQueueRoomVideo(room, videoEntry, { enqueue = false } = {}) {
  if (!room || !videoEntry) {
    return { queued: false, video: null, queuedEntry: null };
  }
  if (enqueue) {
    const queuedEntry = queueRoomVideoEntry(room, videoEntry);
    return {
      queued: true,
      video: room.video,
      queuedEntry
    };
  }
  const video = setRoomCurrentVideo(room, videoEntry);
  return {
    queued: false,
    video,
    queuedEntry: null
  };
}

function setRoomVideoFromYouTubeProxy(room, videoId, direct, uploadedBy) {
  const proxy = registerYouTubeProxyStream(videoId, direct);
  return setRoomCurrentVideo(room, {
    id: randomToken(),
    sourceType: "file",
    youtubeId: videoId,
    src: proxy.src,
    filename: direct.cleanFileName,
    mimeType: direct.mimeType,
    size: Number(direct.estimatedBytes || 0),
    uploadedBy,
    uploadedAt: Date.now(),
    duration: direct.durationSec,
    isYouTubeCached: false,
    isYouTubeProxy: true
  });
}

function setRoomVideoFromYouTubeEmbed(room, videoId, uploadedBy, label = "", duration = 0) {
  const entry = createYouTubeEmbedVideoEntry(videoId, uploadedBy, label, duration);
  if (!entry) {
    return null;
  }
  return setRoomCurrentVideo(room, entry);
}

function ensureRoomVideoRuntimeState(room) {
  const sourceType = String(room?.video?.sourceType || "").toLowerCase();
  const src = String(room?.video?.src || "");
  const isAllowedFileSrc =
    src.startsWith(ROOM_VIDEO_PUBLIC_PREFIX) ||
    src.startsWith(ROOM_VIDEO_BLOB_API_PREFIX) ||
    src.startsWith("/api/youtube-proxy/");
  if (room?.video && sourceType === "file" && src && !isAllowedFileSrc) {
    room.video = null;
    room.videoSync = null;
  }
  if (!room.video) {
    room.video = null;
  }
  if (!room.videoSync) {
    room.videoSync = null;
  }
  normalizeRoomVideoCollections(room);
  room.videoQueue = room.videoQueue
    .map((entry) => cloneRoomVideoEntry(entry))
    .filter((entry) => Boolean(entry?.src));
  room.videoHistory = room.videoHistory
    .map((entry) => cloneRoomVideoEntry(entry))
    .filter((entry) => Boolean(entry?.src));
  trimRoomVideoCollections(room);
}

function getRoomVideoEffectiveTime(room, atTimestamp = Date.now()) {
  if (!room || !room.video || !room.videoSync) {
    return 0;
  }
  const sync = room.videoSync;
  const base = clampRoomVideoTime(room, sync.baseTime);
  if (!sync.playing) {
    return base;
  }
  const playbackRate = normalizeRoomVideoPlaybackRate(sync.playbackRate);
  const elapsedSec = Math.max(0, (Number(atTimestamp) - Number(sync.updatedAt || 0)) / 1000);
  return clampRoomVideoTime(room, base + elapsedSec * playbackRate);
}

function formatRoomVideo(room) {
  if (!room?.video) {
    return null;
  }
  ensureRoomVideoRuntimeState(room);
  return formatRoomVideoEntry(room.video, room, room.videoSync);
}

function formatRoomVideoQueue(room) {
  if (!room) {
    return [];
  }
  ensureRoomVideoRuntimeState(room);
  return room.videoQueue.map((entry, index) => ({
    ...formatRoomVideoEntry(entry, room, null),
    queueIndex: index
  }));
}

function formatRoom(room, viewer = null) {
  ensureRoomRuntimeState(room);
  return {
    code: room.code,
    name: room.name,
    host: room.host,
    members: Array.from(room.members),
    createdAt: room.createdAt,
    pendingCount: room.joinRequests.size,
    hasPendingRequest: viewer ? room.joinRequests.has(viewer) : false,
    video: formatRoomVideo(room),
    videoQueue: formatRoomVideoQueue(room),
    videoHistoryCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
  };
}

function formatRoomSummary(room, viewer) {
  const approvedUsers = room.approvedUsers instanceof Set ? room.approvedUsers : room.members;
  return {
    code: room.code,
    name: room.name,
    host: room.host,
    createdAt: room.createdAt,
    memberCount: room.members.size,
    isHost: room.host === viewer,
    isMember: room.members.has(viewer),
    isApproved: approvedUsers.has(viewer),
    hasPendingRequest: room.joinRequests.has(viewer),
    pendingRequests: room.host === viewer ? Array.from(room.joinRequests) : [],
    hasVideoQueue: Array.isArray(room.videoQueue) ? room.videoQueue.length > 0 : false
  };
}

function normalizeClientMessageId(value) {
  const clean = String(value || "").trim();
  if (!clean) {
    return "";
  }
  if (clean.length > CLIENT_MESSAGE_ID_MAX_LENGTH) {
    return "";
  }
  if (!CLIENT_MESSAGE_ID_REGEX.test(clean)) {
    return "";
  }
  return clean;
}

function normalizeReplyToMessageId(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const numberValue = Number(value);
  if (!Number.isInteger(numberValue) || numberValue <= 0) {
    return NaN;
  }
  return numberValue;
}

function normalizeReactionEmoji(value) {
  const clean = String(value || "").trim();
  if (!clean || clean.length > 8) {
    return "";
  }
  return CHAT_REACTION_ALLOWED.has(clean) ? clean : "";
}

function normalizeMessageReactions(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const normalized = {};
  Object.entries(value).forEach(([emojiRaw, usersRaw]) => {
    const emoji = normalizeReactionEmoji(emojiRaw);
    if (!emoji || !Array.isArray(usersRaw)) {
      return;
    }
    const uniqueUsers = new Set();
    usersRaw.forEach((item) => {
      const username = normalizeUsername(item);
      if (isValidUsername(username)) {
        uniqueUsers.add(username);
      }
    });
    if (uniqueUsers.size > 0) {
      normalized[emoji] = Array.from(uniqueUsers).sort((a, b) => a.localeCompare(b));
    }
  });
  return normalized;
}

function findUserMessageById(room, messageId) {
  if (!room || !Number.isInteger(messageId) || messageId <= 0) {
    return null;
  }
  const target = room.messages.find((item) => Number(item?.id || 0) === messageId);
  if (!target || target.type !== "user") {
    return null;
  }
  return target;
}

function buildReplyReference(room, replyToMessageId) {
  if (!Number.isInteger(replyToMessageId) || replyToMessageId <= 0) {
    return null;
  }
  const target = room.messages.find((item) => item.id === replyToMessageId);
  if (!target || target.type !== "user") {
    return null;
  }
  return {
    id: target.id,
    user: target.user || "",
    text: String(target.text || "").slice(0, REPLY_PREVIEW_MAX_LENGTH)
  };
}

function ensureRoomRuntimeState(room) {
  if (!room.approvedUsers) {
    room.approvedUsers = new Set(room.members);
  }
  if (!room.messageFloorByUser) {
    room.messageFloorByUser = new Map();
  }
  if (!(room.clientMessageLog instanceof Map)) {
    room.clientMessageLog = new Map();
  }
  if (Array.isArray(room.messages)) {
    room.messages.forEach((message) => {
      if (!message || message.type !== "user") {
        return;
      }
      message.reactions = normalizeMessageReactions(message.reactions);
    });
  }
  ensureRoomVideoRuntimeState(room);
}

function pruneClientMessageLog(room) {
  if (!(room.clientMessageLog instanceof Map) || room.clientMessageLog.size === 0) {
    return;
  }
  const cutoff = Date.now() - MESSAGE_DEDUP_TTL_MS;
  room.clientMessageLog.forEach((entry, key) => {
    if (!entry || Number(entry.timestamp || 0) < cutoff) {
      room.clientMessageLog.delete(key);
    }
  });
}

function pushSystemMessage(room, key, payload = {}) {
  const id = room.nextMessageId;
  const message = {
    id,
    type: "system",
    key,
    payload,
    text: "",
    timestamp: Date.now()
  };
  room.messages.push(message);
  room.nextMessageId += 1;
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomEvent(room.code, "chat_message", { message });
  broadcastRoomTouch(room, "message_system", { messageId: id });
  return id;
}

function pushUserMessage(room, username, text, replyTo = null) {
  const id = room.nextMessageId;
  const message = {
    id,
    type: "user",
    user: username,
    text,
    replyTo: replyTo ? { ...replyTo } : null,
    reactions: {},
    timestamp: Date.now()
  };
  room.messages.push(message);
  room.nextMessageId += 1;
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomEvent(room.code, "chat_message", { message });
  broadcastRoomTouch(room, "message_user", { messageId: id });
  return id;
}

function cancelRoomDeletionTimer(roomCode) {
  const code = String(roomCode || "").trim().toUpperCase();
  if (!code) {
    return;
  }
  const timer = roomEmptyDeletionTimers.get(code);
  if (timer) {
    clearTimeout(timer);
    roomEmptyDeletionTimers.delete(code);
  }
}

function scheduleRoomDeletionIfEmpty(room) {
  if (!room || !room.code) {
    return;
  }
  cancelRoomDeletionTimer(room.code);
  const code = String(room.code).trim().toUpperCase();
  const timer = setTimeout(() => {
    roomEmptyDeletionTimers.delete(code);
    const currentRoom = rooms.get(code);
    if (!currentRoom) {
      return;
    }
    ensureRoomRuntimeState(currentRoom);
    if (currentRoom.members.size > 0) {
      return;
    }
    removeRoomVideoAsset(currentRoom, { clearQueue: true, clearHistory: true });
    broadcastRoomEvent(code, "room_closed", { reason: "empty" });
    rooms.delete(code);
    scheduleRuntimeSnapshotPersistence();
  }, ROOM_EMPTY_DELETE_DELAY_MS);
  roomEmptyDeletionTimers.set(code, timer);
}

function joinUserToRoom(room, username) {
  cancelRoomDeletionTimer(room.code);
  room.members.add(username);
  room.approvedUsers.add(username);
  room.joinRequests.delete(username);
  room.messageFloorByUser.set(username, 0);
  pushSystemMessage(room, "room_joined", { user: username });
  if (room.pendingHostRestore === username && room.host !== username) {
    const previous = room.host;
    room.host = username;
    room.pendingHostRestore = null;
    pushSystemMessage(room, "host_changed", { user: username, previous });
  }
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "member_joined", { username });
}

function parseRoomPath(pathname) {
  const match = pathname.match(/^\/api\/rooms\/([A-Za-z0-9]+)(?:\/(messages|kick|host|requests|request-join|leave|video|video-sync|video-source|video-queue))?$/);
  if (!match) {
    return null;
  }
  return {
    code: match[1].toUpperCase(),
    action: match[2] || null
  };
}

function removeMemberFromRoom(room, username) {
  ensureRoomRuntimeState(room);
  room.members.delete(username);
  room.joinRequests.delete(username);
  disconnectRoomMemberSockets(room.code, username, "removed_from_room");

  if (room.members.size === 0) {
    scheduleRoomDeletionIfEmpty(room);
    scheduleRuntimeSnapshotPersistence();
    broadcastRoomTouch(room, "member_left", { username });
    return { deleted: false, room };
  }

  if (room.host === username) {
    room.pendingHostRestore = username;
    const nextHost = Array.from(room.members)[0] || null;
    room.host = nextHost;
    if (nextHost) {
      pushSystemMessage(room, "host_changed", { user: nextHost, previous: username });
    }
  }

  pushSystemMessage(room, "room_left", { user: username });
  scheduleRuntimeSnapshotPersistence();
  broadcastRoomTouch(room, "member_left", { username });
  return { deleted: false, room };
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    const MAX_BODY_SIZE = 1024 * 512;
    let body = "";
    let byteLength = 0;
    let settled = false;
    const fail = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      reject(error);
    };
    const done = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(value);
    };
    req.on("data", (chunk) => {
      if (settled) {
        return;
      }
      byteLength += chunk.length;
      if (byteLength > MAX_BODY_SIZE) {
        fail(new Error("Payload too large"));
        req.resume();
        return;
      }
      body += chunk.toString("utf8");
    });
    req.on("end", () => {
      if (settled) {
        return;
      }
      if (!body) {
        done({});
        return;
      }
      try {
        done(JSON.parse(body));
      } catch (error) {
        fail(new Error("Invalid JSON"));
      }
    });
    req.on("error", fail);
  });
}

function parseMultipartFormData(req, maxBytes = ROOM_VIDEO_MAX_BYTES) {
  return new Promise((resolve, reject) => {
    const contentType = String(req.headers["content-type"] || "").toLowerCase();
    if (!contentType.includes("multipart/form-data")) {
      reject(new Error("Invalid multipart form data"));
      return;
    }

    const maxPayloadBytes = maxBytes + ROOM_VIDEO_MULTIPART_OVERHEAD_BYTES;
    const contentLength = Number(req.headers["content-length"] || 0);
    if (Number.isFinite(contentLength) && contentLength > maxPayloadBytes) {
      reject(new Error("Payload too large"));
      req.resume();
      return;
    }

    let parser = null;
    try {
      parser = Busboy({
        headers: req.headers,
        limits: {
          files: 1,
          fileSize: maxBytes + 1,
          fields: 12,
          fieldSize: 128 * 1024,
          parts: 16
        }
      });
    } catch (_error) {
      reject(new Error("Invalid multipart form data"));
      return;
    }

    const fields = {};
    let uploadedFile = null;
    let extraFileDetected = false;
    const writeTasks = [];
    const tempPaths = [];
    let settled = false;
    const fail = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      for (const tempPath of tempPaths) {
        removeFileIfExists(tempPath);
      }
      reject(error);
    };
    const done = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      resolve(value);
    };

    parser.on("field", (fieldName, value) => {
      if (settled) {
        return;
      }
      const cleanFieldName = String(fieldName || "").trim();
      if (!cleanFieldName || Object.prototype.hasOwnProperty.call(fields, cleanFieldName)) {
        return;
      }
      fields[cleanFieldName] = String(value || "");
    });

    parser.on("file", (fieldName, stream, info) => {
      if (settled) {
        stream.resume();
        return;
      }
      if (uploadedFile) {
        extraFileDetected = true;
        stream.resume();
        return;
      }
      const filename = String(info?.filename || "");
      const contentTypeValue = String(info?.mimeType || info?.mimetype || "application/octet-stream");
      const tempFileName = `upload-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.tmp`;
      const tempPath = path.join(getRoomVideoUploadDir(), tempFileName);
      tempPaths.push(tempPath);
      uploadedFile = {
        fieldName: String(fieldName || "").trim(),
        filename,
        contentType: contentTypeValue,
        tempPath,
        size: 0,
        truncated: false
      };

      const task = new Promise((resolveTask, rejectTask) => {
        let writer = null;
        try {
          writer = fs.createWriteStream(tempPath, { flags: "wx" });
        } catch (error) {
          rejectTask(error);
          stream.resume();
          return;
        }
        stream.on("limit", () => {
          uploadedFile.truncated = true;
        });
        stream.on("data", (chunk) => {
          uploadedFile.size += chunk.length;
        });
        stream.on("error", rejectTask);
        writer.on("error", rejectTask);
        writer.on("finish", resolveTask);
        stream.pipe(writer);
      });
      writeTasks.push(task);
    });

    parser.on("filesLimit", () => {
      extraFileDetected = true;
    });

    parser.on("finish", () => {
      Promise.all(writeTasks)
        .then(() => {
          if (extraFileDetected) {
            fail(new Error("Invalid multipart form data"));
            return;
          }
          if (uploadedFile && (uploadedFile.truncated || uploadedFile.size > maxBytes)) {
            fail(new Error("Payload too large"));
            return;
          }
          done({ fields, files: uploadedFile ? [uploadedFile] : [] });
        })
        .catch((error) => {
          fail(error?.message === "Unexpected end of form" ? new Error("Invalid multipart form data") : error);
        });
    });

    parser.on("error", () => {
      fail(new Error("Invalid multipart form data"));
    });

    req.on("aborted", () => {
      fail(new Error("Invalid multipart form data"));
    });
    req.on("error", fail);
    req.pipe(parser);
  });
}

function sanitizeUploadedFilename(value) {
  const base = path.basename(String(value || "").trim()).replace(/[^\p{L}\p{N}._ -]/gu, "_");
  const collapsed = base.replace(/\s+/g, " ").trim();
  if (!collapsed) {
    return "video";
  }
  return collapsed.slice(0, ROOM_VIDEO_MAX_FILENAME_LENGTH);
}

function normalizeVideoMimeType(value) {
  return String(value || "")
    .split(";")[0]
    .trim()
    .toLowerCase();
}

function normalizeVideoExtension(filename, mimeType) {
  const fromName = path.extname(String(filename || "")).trim().toLowerCase();
  if (fromName === ".mp4" || fromName === ".webm" || fromName === ".ogg") {
    return fromName;
  }
  if (mimeType === "video/webm") {
    return ".webm";
  }
  if (mimeType === "video/ogg") {
    return ".ogg";
  }
  return ".mp4";
}

function extractYouTubeVideoId(input) {
  const raw = String(input || "").trim();
  if (!raw) {
    return "";
  }
  if (YOUTUBE_VIDEO_ID_REGEX.test(raw)) {
    return raw;
  }
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (_error) {
    return "";
  }
  const host = parsed.hostname.toLowerCase();
  const pathSegments = parsed.pathname.split("/").filter(Boolean);
  if (host === "youtu.be") {
    const direct = String(pathSegments[0] || "").trim();
    return YOUTUBE_VIDEO_ID_REGEX.test(direct) ? direct : "";
  }
  const isYouTubeHost =
    host === "youtube.com" ||
    host.endsWith(".youtube.com") ||
    host === "music.youtube.com";
  if (!isYouTubeHost) {
    return "";
  }
  const watchId = String(parsed.searchParams.get("v") || "").trim();
  if (YOUTUBE_VIDEO_ID_REGEX.test(watchId)) {
    return watchId;
  }
  const fromPath = String(pathSegments[1] || "").trim();
  const first = String(pathSegments[0] || "").trim();
  if ((first === "shorts" || first === "embed" || first === "live") && YOUTUBE_VIDEO_ID_REGEX.test(fromPath)) {
    return fromPath;
  }
  return "";
}

function fetchText(url, timeoutMs = YOUTUBE_SEARCH_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const req = https.get(
      url,
      {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9"
        }
      },
      (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const next = new URL(res.headers.location, url).toString();
          res.resume();
          fetchText(next, timeoutMs).then(resolve).catch(reject);
          return;
        }
        if (res.statusCode !== 200) {
          res.resume();
          reject(new Error(`Upstream status ${res.statusCode}`));
          return;
        }
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk;
          if (body.length > 3_000_000) {
            req.destroy(new Error("Upstream payload too large"));
          }
        });
        res.on("end", () => {
          if (settled) {
            return;
          }
          settled = true;
          resolve(body);
        });
      }
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error("Upstream timeout"));
    });
    req.on("error", (error) => {
      if (settled) {
        return;
      }
      settled = true;
      reject(error);
    });
  });
}

function extractFirstYouTubeVideoIdFromSearchHtml(html) {
  const body = String(html || "");
  if (!body) {
    return "";
  }
  const re = /"videoId":"([A-Za-z0-9_-]{11})"/g;
  while (true) {
    const match = re.exec(body);
    if (!match) {
      break;
    }
    const candidate = String(match[1] || "");
    if (YOUTUBE_VIDEO_ID_REGEX.test(candidate)) {
      return candidate;
    }
  }
  return "";
}

async function resolveYouTubeInput(rawInput) {
  const cleanInput = String(rawInput || "").trim().slice(0, YOUTUBE_INPUT_MAX_LENGTH);
  if (cleanInput.length < 2) {
    return null;
  }
  const directId = extractYouTubeVideoId(cleanInput);
  if (directId) {
    return { videoId: directId, label: cleanInput };
  }
  const searchUrl = `https://www.youtube.com/results?search_query=${encodeURIComponent(cleanInput)}`;
  const html = await fetchText(searchUrl);
  const foundId = extractFirstYouTubeVideoIdFromSearchHtml(html);
  if (!foundId) {
    return null;
  }
  return { videoId: foundId, label: cleanInput };
}

async function getYouTubeInfoRobust(videoId) {
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  const playerClients = ["ANDROID", "IOS", "TV", "WEB_EMBEDDED", "WEB"];
  try {
    return await ytdl.getInfo(url, { playerClients });
  } catch (_error) {
    return ytdl.getInfo(url);
  }
}

function getYbdYtdlClient() {
  if (!ybdYtdlModule || typeof ybdYtdlModule.YtdlCore !== "function") {
    return null;
  }
  if (ybdYtdlClient) {
    return ybdYtdlClient;
  }
  try {
    ybdYtdlClient = new ybdYtdlModule.YtdlCore({
      logDisplay: "none",
      noUpdate: true,
      disableRetryRequest: false
    });
    return ybdYtdlClient;
  } catch (_error) {
    ybdYtdlClient = null;
    return null;
  }
}

function toNodeReadableFromYbdStream(stream) {
  if (!stream) {
    return null;
  }
  if (typeof stream.pipe === "function" && typeof stream.on === "function") {
    return stream;
  }
  if (ybdYtdlModule && typeof ybdYtdlModule.toPipeableStream === "function") {
    try {
      const converted = ybdYtdlModule.toPipeableStream(stream);
      if (converted && typeof converted.pipe === "function" && typeof converted.on === "function") {
        return converted;
      }
    } catch (_error) {
      // Try other conversions.
    }
  }
  if (typeof Readable.fromWeb === "function" && stream && typeof stream.getReader === "function") {
    try {
      const converted = Readable.fromWeb(stream);
      if (converted && typeof converted.pipe === "function" && typeof converted.on === "function") {
        return converted;
      }
    } catch (_error) {
      // Ignore unsupported stream conversion.
    }
  }
  return null;
}

function estimateYouTubeFormatSizeBytes(format) {
  const direct = Number(format?.contentLength || 0);
  if (Number.isFinite(direct) && direct > 0) {
    return direct;
  }
  const bitrate = Number(format?.bitrate || format?.averageBitrate || 0);
  const approxDurationMs = Number(format?.approxDurationMs || 0);
  if (Number.isFinite(bitrate) && bitrate > 0 && Number.isFinite(approxDurationMs) && approxDurationMs > 0) {
    return Math.floor((bitrate * (approxDurationMs / 1000)) / 8);
  }
  return 0;
}

function compareYouTubeFormats(a, b) {
  const heightA = Number(a?.height || 0);
  const heightB = Number(b?.height || 0);
  if (heightA !== heightB) {
    return heightB - heightA;
  }
  const bitrateA = Number(a?.bitrate || a?.averageBitrate || 0);
  const bitrateB = Number(b?.bitrate || b?.averageBitrate || 0);
  if (bitrateA !== bitrateB) {
    return bitrateB - bitrateA;
  }
  return Number(b?.itag || 0) - Number(a?.itag || 0);
}

function selectYouTubeDownloadFormats(info) {
  const source = Array.isArray(info?.formats) ? info.formats : [];
  const candidates = source.filter((item) => {
    if (!item || !item.hasVideo || !item.hasAudio) {
      return false;
    }
    const container = String(item.container || "").toLowerCase();
    return container === "mp4" || container === "webm" || container === "ogg";
  });
  const withSize = candidates.map((item) => ({
    format: item,
    estimatedBytes: estimateYouTubeFormatSizeBytes(item)
  }));
  const eligible = withSize
    .filter((entry) => entry.estimatedBytes <= 0 || entry.estimatedBytes <= ROOM_VIDEO_MAX_BYTES)
    .sort((a, b) => compareYouTubeFormats(a.format, b.format))
    .map((entry) => entry.format);
  const onlyTooLarge = withSize.length > 0 && eligible.length === 0;
  return { formats: eligible, onlyTooLarge };
}

function selectYouTubePlayableFormats(info) {
  const source = Array.isArray(info?.formats) ? info.formats : [];
  return source
    .filter((item) => {
      if (!item || !item.hasVideo || !item.hasAudio || !item.url) {
        return false;
      }
      const container = String(item.container || "").toLowerCase();
      if (container !== "mp4" && container !== "webm" && container !== "ogg") {
        return false;
      }
      if (item.isHLS || item.isDashMPD) {
        return false;
      }
      return true;
    })
    .sort(compareYouTubeFormats);
}

async function getYouTubeDirectPlayableSource(videoId) {
  const info = await getYouTubeInfoRobust(videoId);
  const formats = selectYouTubePlayableFormats(info);
  if (!formats.length) {
    const formatError = new Error("No supported direct YouTube format");
    formatError.code = "YOUTUBE_FORMAT_UNSUPPORTED";
    throw formatError;
  }
  const format = formats[0];
  const container = String(format?.container || "mp4").toLowerCase();
  return {
    itag: Number(format?.itag || 0),
    cleanFileName: sanitizeUploadedFilename(info?.videoDetails?.title || `youtube-${videoId}`),
    durationSec: normalizeRoomVideoDuration(Number(info?.videoDetails?.lengthSeconds || 0)),
    mimeType: `video/${container === "ogg" ? "ogg" : container === "webm" ? "webm" : "mp4"}`,
    estimatedBytes: estimateYouTubeFormatSizeBytes(format)
  };
}

function canUseYtDlpBinaryPath(binaryPath) {
  const cleanPath = String(binaryPath || "").trim();
  if (!cleanPath) {
    return false;
  }
  const looksLikePath = cleanPath.includes("/") || cleanPath.includes("\\") || path.isAbsolute(cleanPath);
  if (!looksLikePath) {
    return false;
  }
  try {
    const stat = fs.statSync(cleanPath);
    if (!stat.isFile()) {
      return false;
    }
    if (process.platform !== "win32") {
      fs.chmodSync(cleanPath, 0o755);
    }
    return true;
  } catch (_error) {
    return false;
  }
}

async function downloadRemoteFileToPath(url, targetPath, timeoutMs = YT_DLP_AUTO_DOWNLOAD_TIMEOUT_MS) {
  const cleanUrl = String(url || "").trim();
  const cleanTargetPath = String(targetPath || "").trim();
  if (!cleanUrl || !cleanTargetPath) {
    return false;
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const tempPath = `${cleanTargetPath}.tmp-${Date.now()}-${crypto.randomBytes(4).toString("hex")}`;
  try {
    const response = await fetch(cleanUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "user-agent": "bednanl3b-ytdlp-installer/1.0"
      }
    });
    if (!response.ok || !response.body) {
      throw new Error(`HTTP ${response.status}`);
    }
    fs.mkdirSync(path.dirname(cleanTargetPath), { recursive: true });
    const source = typeof Readable.fromWeb === "function"
      ? Readable.fromWeb(response.body)
      : null;
    if (!source) {
      throw new Error("Web stream is unsupported in this runtime");
    }
    await streamPipeline(source, fs.createWriteStream(tempPath, { flags: "w" }));
    const stats = fs.statSync(tempPath);
    if (!Number.isFinite(Number(stats.size)) || Number(stats.size) < 512 * 1024) {
      throw new Error("Downloaded file is too small to be a valid yt-dlp binary");
    }
    const probeBuffer = Buffer.alloc(256);
    const fd = fs.openSync(tempPath, "r");
    try {
      fs.readSync(fd, probeBuffer, 0, probeBuffer.length, 0);
    } finally {
      fs.closeSync(fd);
    }
    const probeText = probeBuffer.toString("utf8").toLowerCase();
    if (probeText.includes("<html") || probeText.includes("<!doctype")) {
      throw new Error("Downloaded content looks like an HTML error page");
    }
    fs.renameSync(tempPath, cleanTargetPath);
    if (process.platform !== "win32") {
      fs.chmodSync(cleanTargetPath, 0o755);
    }
    return true;
  } catch (_error) {
    removeFileIfExists(tempPath);
    return false;
  } finally {
    clearTimeout(timer);
  }
}

async function ensureYtDlpBinaryAvailable() {
  if (canUseYtDlpBinaryPath(YT_DLP_BINARY)) {
    return YT_DLP_BINARY;
  }
  if (canUseYtDlpBinaryPath(YT_DLP_EMBEDDED_BINARY)) {
    return YT_DLP_EMBEDDED_BINARY;
  }
  if (canUseYtDlpBinaryPath(YT_DLP_AUTO_BINARY_PATH)) {
    return YT_DLP_AUTO_BINARY_PATH;
  }
  if (!YT_DLP_AUTO_DOWNLOAD_URLS.length) {
    return "";
  }
  if (!ytDlpAutoInstallPromise) {
    ytDlpAutoInstallPromise = (async () => {
      for (const url of YT_DLP_AUTO_DOWNLOAD_URLS) {
        const installed = await downloadRemoteFileToPath(
          url,
          YT_DLP_AUTO_BINARY_PATH,
          YT_DLP_AUTO_DOWNLOAD_TIMEOUT_MS
        );
        if (installed && canUseYtDlpBinaryPath(YT_DLP_AUTO_BINARY_PATH)) {
          return YT_DLP_AUTO_BINARY_PATH;
        }
      }
      return "";
    })().finally(() => {
      ytDlpAutoInstallPromise = null;
    });
  }
  return ytDlpAutoInstallPromise;
}

function runYtDlp(args, timeoutMs = YOUTUBE_DOWNLOAD_TIMEOUT_MS) {
  const ensureCommandIsExecutable = (command) => {
    if (process.platform === "win32") {
      return;
    }
    const cleanCommand = String(command || "").trim();
    if (!cleanCommand) {
      return;
    }
    const looksLikeFilePath = cleanCommand.includes("/") || cleanCommand.includes("\\") || path.isAbsolute(cleanCommand);
    if (!looksLikeFilePath || !fs.existsSync(cleanCommand)) {
      return;
    }
    try {
      fs.chmodSync(cleanCommand, 0o755);
    } catch (_error) {
      // Ignore chmod issues and let spawn report a real execution error.
    }
  };

  const commandCandidates = [];
  const commandCandidateKeys = new Set();
  const pushCommandCandidate = (command, baseArgs = []) => {
    const cleanCommand = String(command || "").trim();
    if (!cleanCommand) {
      return;
    }
    const normalizedArgs = Array.isArray(baseArgs) ? baseArgs.map((item) => String(item)) : [];
    const key = `${cleanCommand}\u0000${normalizedArgs.join("\u0000")}`;
    if (commandCandidateKeys.has(key)) {
      return;
    }
    commandCandidateKeys.add(key);
    commandCandidates.push([cleanCommand, normalizedArgs]);
  };
  if (ytDlpCommandOverride) {
    pushCommandCandidate(ytDlpCommandOverride[0], ytDlpCommandOverride[1]);
  } else {
    pushCommandCandidate(YT_DLP_BINARY, []);
    pushCommandCandidate(YT_DLP_EMBEDDED_BINARY, []);
    pushCommandCandidate("python3", ["-m", "yt_dlp"]);
    pushCommandCandidate("python", ["-m", "yt_dlp"]);
  }
  const autoBinaryCandidatePromise = ensureYtDlpBinaryAvailable().catch(() => "");

  const runWithCommand = (command, baseArgs) => new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    let settled = false;
    let timer = null;
    const proxyArgs = YOUTUBE_OUTBOUND_PROXY ? ["--proxy", YOUTUBE_OUTBOUND_PROXY] : [];
    const mergedArgs = [...baseArgs, ...proxyArgs, ...args];
    let child = null;
    ensureCommandIsExecutable(command);
    try {
      child = spawn(command, mergedArgs, { windowsHide: true });
    } catch (error) {
      const wrapped = new Error(`yt-dlp spawn failed: ${error?.message || error}`);
      wrapped.code = "YT_DLP_UNAVAILABLE";
      reject(wrapped);
      return;
    }

    const finish = (error, result = null) => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer) {
        clearTimeout(timer);
      }
      if (error) {
        reject(error);
        return;
      }
      resolve(result);
    };

    timer = setTimeout(() => {
      if (child && !child.killed) {
        child.kill("SIGKILL");
      }
      const timeoutError = new Error("yt-dlp timeout");
      timeoutError.code = "YT_DLP_TIMEOUT";
      finish(timeoutError);
    }, timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += String(chunk || "");
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk || "");
    });

    child.on("error", (error) => {
      const wrapped = new Error(`yt-dlp execution failed: ${error?.message || error}`);
      const childErrorCode = String(error?.code || "").trim().toUpperCase();
      wrapped.code = childErrorCode === "ENOENT" || childErrorCode === "EACCES" || childErrorCode === "EPERM" || childErrorCode === "ENOEXEC"
        ? "YT_DLP_UNAVAILABLE"
        : "YT_DLP_FAILED";
      finish(wrapped);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        const runError = new Error(`yt-dlp exited with code ${code}: ${stderr.trim() || stdout.trim()}`);
        const lowerOutput = `${stderr}\n${stdout}`.toLowerCase();
        const looksUnavailable = Number(code) === 126
          || Number(code) === 127
          || lowerOutput.includes("exec format error")
          || lowerOutput.includes("not found");
        runError.code = looksUnavailable ? "YT_DLP_UNAVAILABLE" : "YT_DLP_FAILED";
        finish(runError);
        return;
      }
      finish(null, { stdout, stderr });
    });
  });

  const attempt = async () => {
    const autoBinaryCandidate = await autoBinaryCandidatePromise;
    if (autoBinaryCandidate) {
      pushCommandCandidate(autoBinaryCandidate, []);
    }
    let unavailableCount = 0;
    let lastError = null;
    for (const candidate of commandCandidates) {
      const [command, baseArgs] = candidate;
      try {
        const result = await runWithCommand(command, baseArgs);
        ytDlpCommandOverride = [command, baseArgs];
        return result;
      } catch (error) {
        lastError = error;
        if (error?.code === "YT_DLP_UNAVAILABLE") {
          unavailableCount += 1;
          continue;
        }
        throw error;
      }
    }
    const unavailable = new Error("yt-dlp is unavailable");
    unavailable.code = unavailableCount === commandCandidates.length ? "YT_DLP_UNAVAILABLE" : (lastError?.code || "YT_DLP_FAILED");
    throw unavailable;
  };

  return attempt();
}

async function downloadYouTubeVideoToLocalFileWithYtDlp(videoId) {
  const watchUrl = `https://www.youtube.com/watch?v=${videoId}`;
  const outputBase = `yt-${videoId}-${Date.now()}-${crypto.randomBytes(4).toString("hex")}`;
  const outputTemplate = path.join(getRoomVideoUploadDir(), `${outputBase}.%(ext)s`);

  let infoPayload = null;
  try {
    const meta = await runYtDlp(["--dump-single-json", "--no-warnings", "--no-playlist", watchUrl]);
    infoPayload = JSON.parse(String(meta.stdout || "{}"));
  } catch (error) {
    if (error?.code === "YT_DLP_UNAVAILABLE") {
      throw error;
    }
    // Continue without metadata if extraction fails; download attempt may still succeed.
  }

  await runYtDlp([
    "--no-warnings",
    "--no-playlist",
    "--max-filesize",
    String(ROOM_VIDEO_MAX_BYTES),
    "-f",
    "best[ext=mp4][acodec!=none][vcodec!=none]/best[acodec!=none][vcodec!=none]",
    "-o",
    outputTemplate,
    watchUrl
  ]);

  const candidates = fs
    .readdirSync(getRoomVideoUploadDir())
    .filter((name) => name.startsWith(`${outputBase}.`))
    .sort((a, b) => a.localeCompare(b));
  if (!candidates.length) {
    const missing = new Error("yt-dlp output not found");
    missing.code = "YT_DLP_FAILED";
    throw missing;
  }

  const storedFileName = candidates[0];
  const absolutePath = path.join(getRoomVideoUploadDir(), storedFileName);
  const stats = fs.statSync(absolutePath);
  const downloadedBytes = Number(stats.size || 0);
  if (downloadedBytes <= 0) {
    try {
      fs.unlinkSync(absolutePath);
    } catch (_error) {
      // Ignore cleanup errors.
    }
    const empty = new Error("yt-dlp produced empty file");
    empty.code = "YT_DLP_FAILED";
    throw empty;
  }
  if (downloadedBytes > ROOM_VIDEO_MAX_BYTES) {
    try {
      fs.unlinkSync(absolutePath);
    } catch (_error) {
      // Ignore cleanup errors.
    }
    const tooLarge = new Error("yt-dlp file too large");
    tooLarge.code = "YOUTUBE_VIDEO_TOO_LARGE";
    throw tooLarge;
  }

  const ext = path.extname(storedFileName).toLowerCase();
  const mimeType = ext === ".webm" ? "video/webm" : ext === ".ogg" ? "video/ogg" : "video/mp4";
  const cleanFileName = sanitizeUploadedFilename(infoPayload?.title || `youtube-${videoId}`);
  const durationSec = normalizeRoomVideoDuration(Number(infoPayload?.duration || 0));
  return {
    storedFileName,
    cleanFileName,
    durationSec,
    downloadedBytes,
    mimeType
  };
}

async function downloadYouTubeVideoToLocalFileWithYbd(videoId) {
  const ybdClient = getYbdYtdlClient();
  if (!ybdClient) {
    const unavailable = new Error("YBD ytdl fallback is unavailable");
    unavailable.code = "YBD_YTDL_UNAVAILABLE";
    throw unavailable;
  }

  const watchUrl = `https://www.youtube.com/watch?v=${videoId}`;
  let info = null;
  try {
    info = await ybdClient.getBasicInfo(watchUrl, {
      filter: "videoandaudio",
      quality: "highest"
    });
  } catch (_error) {
    // Download may still succeed without metadata.
  }

  let pickedContainer = "mp4";
  const candidates = Array.isArray(info?.formats)
    ? info.formats
      .filter((item) => item && item.hasVideo && item.hasAudio)
      .map((item) => ({ format: item, estimatedBytes: estimateYouTubeFormatSizeBytes(item) }))
      .filter((item) => item.estimatedBytes <= 0 || item.estimatedBytes <= ROOM_VIDEO_MAX_BYTES)
      .sort((a, b) => compareYouTubeFormats(a.format, b.format))
    : [];
  if (candidates.length) {
    pickedContainer = String(candidates[0]?.format?.container || "mp4").toLowerCase();
  }
  const extension = pickedContainer === "webm" ? ".webm" : pickedContainer === "ogg" ? ".ogg" : ".mp4";
  const mimeType = extension === ".webm" ? "video/webm" : extension === ".ogg" ? "video/ogg" : "video/mp4";

  const cleanFileName = sanitizeUploadedFilename(info?.videoDetails?.title || `youtube-${videoId}`);
  const durationSec = normalizeRoomVideoDuration(Number(info?.videoDetails?.lengthSeconds || 0));
  const storedFileName = `yt-${videoId}-${Date.now()}-${crypto.randomBytes(4).toString("hex")}${extension}`;
  const absolutePath = path.join(getRoomVideoUploadDir(), storedFileName);
  let downloadedBytes = 0;

  await new Promise((resolve, reject) => {
    let writer = null;
    let stream = null;
    let timer = null;
    let done = false;
    const finish = (error) => {
      if (done) {
        return;
      }
      done = true;
      if (timer) {
        clearTimeout(timer);
      }
      if (error) {
        if (stream && typeof stream.destroy === "function") {
          stream.destroy();
        }
        if (writer && typeof writer.destroy === "function") {
          writer.destroy();
        }
        removeFileIfExists(absolutePath);
        reject(error);
        return;
      }
      resolve();
    };

    writer = fs.createWriteStream(absolutePath, { flags: "wx" });
    writer.on("error", (error) => finish(error));
    writer.on("finish", () => finish());

    Promise.resolve(
      ybdClient.download(watchUrl, {
        filter: "videoandaudio",
        quality: "highest"
      })
    )
      .then((downloadStream) => {
        stream = toNodeReadableFromYbdStream(downloadStream);
        if (!stream) {
          const streamError = new Error("YBD stream conversion failed");
          streamError.code = "YBD_STREAM_UNSUPPORTED";
          finish(streamError);
          return;
        }
        timer = setTimeout(() => {
          const timeoutError = new Error("YouTube download timeout");
          timeoutError.code = "YOUTUBE_DOWNLOAD_TIMEOUT";
          finish(timeoutError);
        }, YOUTUBE_DOWNLOAD_TIMEOUT_MS);

        stream.on("data", (chunk) => {
          downloadedBytes += chunk.length;
          if (downloadedBytes > ROOM_VIDEO_MAX_BYTES) {
            const sizeError = new Error("YouTube video exceeds max size");
            sizeError.code = "YOUTUBE_VIDEO_TOO_LARGE";
            finish(sizeError);
          }
        });
        stream.on("error", (error) => finish(error));
        stream.pipe(writer);
      })
      .catch((error) => finish(error));
  });

  if (downloadedBytes <= 0) {
    removeFileIfExists(absolutePath);
    const emptyError = new Error("YBD produced empty download");
    emptyError.code = "YBD_DOWNLOAD_FAILED";
    throw emptyError;
  }

  return {
    storedFileName,
    cleanFileName,
    durationSec,
    downloadedBytes,
    mimeType
  };
}

async function downloadYouTubeVideoToLocalFile(videoId) {
  const info = await getYouTubeInfoRobust(videoId);
  const picked = selectYouTubeDownloadFormats(info);
  if (picked.onlyTooLarge) {
    const tooLargeError = new Error("YouTube video is too large");
    tooLargeError.code = "YOUTUBE_VIDEO_TOO_LARGE";
    throw tooLargeError;
  }
  if (!picked.formats.length) {
    const formatError = new Error("No supported YouTube format");
    formatError.code = "YOUTUBE_FORMAT_UNSUPPORTED";
    throw formatError;
  }

  const cleanFileName = sanitizeUploadedFilename(info?.videoDetails?.title || `youtube-${videoId}`);
  const durationSec = normalizeRoomVideoDuration(Number(info?.videoDetails?.lengthSeconds || 0));
  let lastError = null;

  for (const format of picked.formats) {
    const container = String(format?.container || "mp4").toLowerCase();
    const extension = container === "webm" ? ".webm" : container === "ogg" ? ".ogg" : ".mp4";
    const storedFileName = `yt-${videoId}-${Date.now()}-${crypto.randomBytes(4).toString("hex")}${extension}`;
    const absolutePath = path.join(getRoomVideoUploadDir(), storedFileName);
    let downloadedBytes = 0;
    try {
      await new Promise((resolve, reject) => {
        let writer = null;
        let stream = null;
        let timer = null;
        let done = false;
        const finish = (error) => {
          if (done) {
            return;
          }
          done = true;
          if (timer) {
            clearTimeout(timer);
          }
          if (error) {
            if (stream && typeof stream.destroy === "function") {
              stream.destroy();
            }
            if (writer && typeof writer.destroy === "function") {
              writer.destroy();
            }
            try {
              if (fs.existsSync(absolutePath)) {
                fs.unlinkSync(absolutePath);
              }
            } catch (_unlinkError) {
              // Ignore file cleanup errors.
            }
            reject(error);
            return;
          }
          resolve();
        };

        writer = fs.createWriteStream(absolutePath, { flags: "wx" });
        stream = ytdl.downloadFromInfo(info, { format });
        timer = setTimeout(() => {
          const timeoutError = new Error("YouTube download timeout");
          timeoutError.code = "YOUTUBE_DOWNLOAD_TIMEOUT";
          finish(timeoutError);
        }, YOUTUBE_DOWNLOAD_TIMEOUT_MS);

        stream.on("data", (chunk) => {
          downloadedBytes += chunk.length;
          if (downloadedBytes > ROOM_VIDEO_MAX_BYTES) {
            const sizeError = new Error("YouTube video exceeds max size");
            sizeError.code = "YOUTUBE_VIDEO_TOO_LARGE";
            finish(sizeError);
          }
        });
        stream.on("error", (error) => finish(error));
        writer.on("error", (error) => finish(error));
        writer.on("finish", () => finish());
        stream.pipe(writer);
      });

      return {
        storedFileName,
        cleanFileName,
        durationSec,
        downloadedBytes,
        mimeType: `video/${container === "ogg" ? "ogg" : container === "webm" ? "webm" : "mp4"}`
      };
    } catch (error) {
      lastError = error;
      if (error?.code === "YOUTUBE_VIDEO_TOO_LARGE") {
        continue;
      }
      // Try the next compatible format when download fails.
      continue;
    }
  }

  try {
    return await downloadYouTubeVideoToLocalFileWithYtDlp(videoId);
  } catch (ytDlpError) {
    if (ytDlpError?.code === "YOUTUBE_VIDEO_TOO_LARGE") {
      throw ytDlpError;
    }
    if (ytDlpError?.code === "YT_DLP_UNAVAILABLE") {
      console.warn("yt-dlp is not available on this host.");
    } else {
      console.warn("yt-dlp fallback download failed:", ytDlpError?.message || ytDlpError);
    }
  }

  let ybdFallbackError = null;
  try {
    return await downloadYouTubeVideoToLocalFileWithYbd(videoId);
  } catch (ybdError) {
    ybdFallbackError = ybdError;
    if (ybdError?.code === "YOUTUBE_VIDEO_TOO_LARGE") {
      throw ybdError;
    }
    if (ybdError?.code === "YBD_YTDL_UNAVAILABLE") {
      console.warn("YBD ytdl fallback is unavailable on this host.");
    } else {
      console.warn("YBD ytdl fallback download failed:", ybdError?.message || ybdError);
    }
  }

  if (lastError) {
    throw lastError;
  }
  if (ybdFallbackError) {
    throw ybdFallbackError;
  }
  const fallbackError = new Error("YouTube download failed");
  fallbackError.code = "YOUTUBE_DOWNLOAD_FAILED";
  throw fallbackError;
}

function getBearerToken(req) {
  const auth = req.headers.authorization || "";
  if (!auth.startsWith("Bearer ")) {
    return null;
  }
  return auth.slice(7).trim();
}

function safeDecodeURIComponent(value) {
  try {
    return decodeURIComponent(value);
  } catch (_error) {
    return null;
  }
}

function getUserFromRequest(req) {
  const token = getBearerToken(req);
  const username = getSessionUsername(token);
  if (!username) {
    return null;
  }
  touchUser(username);
  return username;
}

function isBlockedStaticPath(relativePath) {
  const normalized = String(relativePath || "")
    .replace(/\\/g, "/")
    .replace(/^\/+/, "")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === ".") {
    return false;
  }

  const segments = normalized.split("/").filter(Boolean);
  if (segments.length === 0) {
    return false;
  }
  if (segments.some((part) => part.startsWith("."))) {
    return true;
  }
  if (BLOCKED_STATIC_DIRS.has(segments[0])) {
    return true;
  }
  return BLOCKED_STATIC_FILES.has(segments[segments.length - 1]);
}

function serveStatic(pathname, req, res) {
  const method = req.method === "HEAD" ? "HEAD" : req.method;
  if (method !== "GET" && method !== "HEAD") {
    res.writeHead(
      405,
      securityHeaders({
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store",
        Allow: "GET, HEAD"
      })
    );
    res.end("405 - Method not allowed");
    return;
  }

  const urlPath = pathname === "/" ? "/index.html" : pathname;
  const decodedPath = safeDecodeURIComponent(urlPath);
  if (decodedPath === null) {
    res.writeHead(
      400,
      securityHeaders({
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store"
      })
    );
    res.end("400 - Bad request");
    return;
  }

  let filePath = "";
  let relativePath = "";
  let skipBlockedPathCheck = false;
  if (decodedPath.startsWith(ROOM_VIDEO_PUBLIC_PREFIX)) {
    const storedFileName = path.basename(decodedPath.slice(ROOM_VIDEO_PUBLIC_PREFIX.length));
    if (!storedFileName) {
      res.writeHead(
        403,
        securityHeaders({
          "Content-Type": "text/plain; charset=utf-8",
          "Cache-Control": "no-store"
        })
      );
      res.end("403 - Forbidden");
      return;
    }
    filePath = path.join(getRoomVideoUploadDir(), storedFileName);
    relativePath = path.relative(getRoomVideoUploadDir(), filePath);
    skipBlockedPathCheck = true;
  } else {
    const safePath = path.normalize(decodedPath).replace(/^([/\\])+/, "");
    filePath = path.join(ROOT, safePath);
    relativePath = path.relative(ROOT, filePath);
  }

  if (
    !relativePath ||
    relativePath.startsWith("..") ||
    path.isAbsolute(relativePath) ||
    (!skipBlockedPathCheck && isBlockedStaticPath(relativePath))
  ) {
    res.writeHead(
      403,
      securityHeaders({
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store"
      })
    );
    res.end("403 - Forbidden");
    return;
  }

  fs.stat(filePath, (statError, stats) => {
    if (statError || !stats.isFile()) {
      res.writeHead(
        404,
        securityHeaders({
          "Content-Type": "text/plain; charset=utf-8",
          "Cache-Control": "no-store"
        })
      );
      res.end("404 - File not found");
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    const cacheControl =
      ext === ".html" || ext === ".js" || ext === ".css"
        ? "no-store"
        : "public, max-age=300, must-revalidate";
    const isVideo = contentType.startsWith("video/");
    const rangeHeader = String(req.headers.range || "").trim();

    if (isVideo && rangeHeader) {
      const match = rangeHeader.match(/^bytes=(\d*)-(\d*)$/i);
      if (!match) {
        res.writeHead(
          416,
          securityHeaders({
            "Content-Range": `bytes */${stats.size}`,
            "Cache-Control": "no-store"
          })
        );
        res.end();
        return;
      }
      let start = 0;
      let end = stats.size - 1;
      if (match[1] === "" && match[2] === "") {
        res.writeHead(
          416,
          securityHeaders({
            "Content-Range": `bytes */${stats.size}`,
            "Cache-Control": "no-store"
          })
        );
        res.end();
        return;
      }
      if (match[1] === "") {
        const suffixLength = Number(match[2]);
        if (!Number.isInteger(suffixLength) || suffixLength <= 0) {
          res.writeHead(
            416,
            securityHeaders({
              "Content-Range": `bytes */${stats.size}`,
              "Cache-Control": "no-store"
            })
          );
          res.end();
          return;
        }
        start = Math.max(0, stats.size - suffixLength);
        end = stats.size - 1;
      } else {
        start = Number(match[1]);
        end = match[2] === "" ? stats.size - 1 : Number(match[2]);
      }
      if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end < start || start >= stats.size) {
        res.writeHead(
          416,
          securityHeaders({
            "Content-Range": `bytes */${stats.size}`,
            "Cache-Control": "no-store"
          })
        );
        res.end();
        return;
      }
      const safeEnd = Math.min(end, stats.size - 1);
      const chunkSize = safeEnd - start + 1;
      res.writeHead(
        206,
        securityHeaders({
          "Content-Type": contentType,
          "Cache-Control": cacheControl,
          "Accept-Ranges": "bytes",
          "Content-Range": `bytes ${start}-${safeEnd}/${stats.size}`,
          "Content-Length": chunkSize
        })
      );
      if (method === "HEAD") {
        res.end();
        return;
      }
      const stream = fs.createReadStream(filePath, { start, end: safeEnd });
      stream.on("error", () => {
        if (!res.headersSent) {
          res.writeHead(500, securityHeaders({ "Cache-Control": "no-store" }));
        }
        res.end();
      });
      stream.pipe(res);
      return;
    }

    res.writeHead(
      200,
      securityHeaders({
        "Content-Type": contentType,
        "Cache-Control": cacheControl,
        "Content-Length": stats.size,
        ...(isVideo ? { "Accept-Ranges": "bytes" } : {})
      })
    );
    if (method === "HEAD") {
      res.end();
      return;
    }
    const stream = fs.createReadStream(filePath);
    stream.on("error", () => {
      if (!res.headersSent) {
        res.writeHead(500, securityHeaders({ "Cache-Control": "no-store" }));
      }
      res.end();
    });
    stream.pipe(res);
  });
}

async function handleApi(req, res) {
  const fullUrl = new URL(req.url, "http://localhost");
  const pathname = fullUrl.pathname;
  if (req.method === "GET" && pathname === "/api/health") {
    sendJson(res, 200, {
      ok: true,
      status: "healthy",
      uptimeSec: Math.floor(process.uptime()),
      nowIso: new Date().toISOString()
    });
    return;
  }
  if ((req.method === "GET" || req.method === "HEAD") && pathname.startsWith("/api/youtube-proxy/")) {
    const streamId = pathname.slice("/api/youtube-proxy/".length).split("/")[0];
    if (!/^[A-Za-z0-9]+$/.test(streamId)) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid video stream link.") });
      return;
    }
    await serveYouTubeProxyStream(req, res, streamId);
    return;
  }
  if ((req.method === "GET" || req.method === "HEAD") && pathname.startsWith(ROOM_VIDEO_BLOB_API_PREFIX)) {
    const blobId = extractRoomVideoBlobIdFromSrc(pathname);
    if (!blobId) {
      sendJson(res, 404, { error: i18n(req, "No room video is available.", "No room video is available.") });
      return;
    }
    await serveRoomVideoBlobFromSql(req, res, blobId);
    return;
  }

  if (req.method === "POST" && pathname === "/api/register") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.REGISTER)) {
      return;
    }
    const { username, password } = await parseBody(req);
    const cleanUser = normalizeUsername(username);
    const cleanPass = String(password || "");

    if (!isValidRegisteredUsername(cleanUser)) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 3 ط£آ¯ط·ع؛ط¢آ½ 30 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½/ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.",
          "Username must be 3-30 characters and use only English letters and numbers."
        )
      });
      return;
    }
    if (!isValidPassword(cleanPass)) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 4 ط£آ¯ط·ع؛ط¢آ½ 128 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.",
          "Password must be 4-128 characters."
        )
      });
      return;
    }
    if (users.has(cleanUser)) {
      recordAudit("auth_register_conflict", cleanUser || "anonymous", cleanUser, {
        ip: getClientIp(req)
      });
      sendJson(res, 409, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Username is already in use.")
      });
      return;
    }

    const passwordRecord = createPasswordRecord(cleanPass);
    const registerCountryCode = getClientCountryCode(req);
    users.set(cleanUser, {
      passwordHash: passwordRecord.passwordHash,
      passwordSalt: passwordRecord.passwordSalt,
      createdAt: Date.now(),
      displayName: cleanUser,
      avatarDataUrl: "",
      countryCode: registerCountryCode
    });
    if (!saveUsers()) {
      users.delete(cleanUser);
      sendJson(res, 500, {
        error: i18n(
          req,
          "Failed to persist account data. Please try again.",
          "Failed to persist account data. Please try again."
        )
      });
      return;
    }
    const token = createSession(cleanUser);
    const registerIp = getClientIp(req);
    recordAudit("auth_register_success", cleanUser, cleanUser, {
      ip: registerIp
    });
    recordAudit("site_presence_enter", cleanUser, cleanUser, {
      ip: registerIp,
      countryCode: registerCountryCode,
      isGuest: false,
      source: "register"
    });
    sendJson(res, 201, { token, username: cleanUser, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/login") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.LOGIN)) {
      return;
    }
    const { username, password } = await parseBody(req);
    const cleanUser = normalizeUsername(username);
    const cleanPass = String(password || "");

    if (!cleanUser || !cleanPass || cleanPass.length > PASSWORD_MAX_LENGTH) {
      recordAudit("auth_login_failure", cleanUser || "anonymous", cleanUser || "", {
        reason: "invalid_input",
        ip: getClientIp(req)
      });
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid login credentials.")
      });
      return;
    }

    if (!users.has(cleanUser)) {
      recordAudit("auth_login_failure", cleanUser || "anonymous", cleanUser || "", {
        reason: "unknown_user",
        ip: getClientIp(req)
      });
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid login credentials.")
      });
      return;
    }
    const user = users.get(cleanUser);
    if (!verifyPassword(cleanUser, user, cleanPass)) {
      recordAudit("auth_login_failure", cleanUser, cleanUser, {
        reason: "bad_password",
        ip: getClientIp(req)
      });
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid login credentials.")
      });
      return;
    }
    if (!isSupervisor(cleanUser) && isBanned(cleanUser)) {
      sendBannedResponse(req, res, cleanUser);
      return;
    }
    const loginCountryCode = updateUserCountryFromRequest(cleanUser, req, { persistRegistered: true });

    const token = createSession(cleanUser);
    const loginIp = getClientIp(req);
    recordAudit("auth_login_success", cleanUser, cleanUser, {
      ip: loginIp
    });
    recordAudit("site_presence_enter", cleanUser, cleanUser, {
      ip: loginIp,
      countryCode: loginCountryCode,
      isGuest: false,
      source: "login"
    });
    sendJson(res, 200, { token, username: cleanUser, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/guest-login") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.GUEST_LOGIN)) {
      return;
    }
    const { name } = await parseBody(req);
    const cleanName = sanitizeGuestDisplayName(name);
    if (cleanName.length < 2 || cleanName.length > 30) {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 2 ط£آ¯ط·ع؛ط¢آ½ 30 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Name must be 2-30 characters.")
      });
      return;
    }
    const username = createGuestUsername(cleanName);
    const guestCountryCode = getClientCountryCode(req);
    guestUsers.set(username, {
      createdAt: Date.now(),
      displayName: cleanName,
      avatarDataUrl: "",
      countryCode: guestCountryCode
    });
    const token = createSession(username, { isGuest: true });
    const guestIp = getClientIp(req);
    recordAudit("auth_guest_login", username, username, {
      ip: guestIp,
      displayName: cleanName
    });
    recordAudit("site_presence_enter", username, username, {
      ip: guestIp,
      countryCode: guestCountryCode,
      displayName: cleanName,
      isGuest: true,
      source: "guest"
    });
    sendJson(res, 200, {
      token,
      username,
      displayName: cleanName,
      isGuest: true,
      announcement: getLiveSiteAnnouncement()
    });
    return;
  }

  if (req.method === "POST" && pathname === "/api/logout") {
    const token = getBearerToken(req);
    if (token) {
      const sessionEntry = sessions.get(token);
      const meta = getSessionMeta(sessionEntry);
      const username = meta.username;
      if (username) {
        const logoutCountryCode = updateUserCountryFromRequest(username, req, {
          persistRegistered: !meta.isGuest
        });
        recordAudit("site_presence_exit", username, username, {
          ip: getClientIp(req),
          countryCode: logoutCountryCode,
          isGuest: Boolean(meta.isGuest),
          source: "logout"
        });
      }
      sessions.delete(token);
      if (meta.isGuest && meta.username && !hasActiveSessionForUser(meta.username, token)) {
        cleanupGuestUser(meta.username);
      }
    }
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "POST" && pathname === "/api/ban-appeals") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.BAN_APPEAL)) {
      return;
    }
    const { username: rawUsername, reason } = await parseBody(req);
    const username = normalizeUsername(rawUsername);
    const cleanReason = String(reason || "").trim();
    if (!isValidUsername(username) || !users.has(username)) {
      sendJson(res, 404, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Account not found.")
      });
      return;
    }
    if (!isBanned(username)) {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "This account is not banned.")
      });
      return;
    }
    if (cleanReason.length < 8 || cleanReason.length > 500) {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 8 ط£آ¯ط·ع؛ط¢آ½ 500 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Appeal reason must be 8-500 characters.")
      });
      return;
    }
    const existing = unbanRequests.get(username);
    const now = Date.now();
    unbanRequests.set(username, {
      username,
      reason: cleanReason,
      status: "pending",
      createdAt: existing?.createdAt || now,
      updatedAt: now,
      reviewedBy: "",
      reviewNote: ""
    });
    saveModeration();
    recordAudit("ban_appeal_submitted", username, username, {
      reasonLength: cleanReason.length,
      ip: getClientIp(req)
    });
    sendJson(res, 202, { ok: true, request: formatUnbanRequest(unbanRequests.get(username)) });
    return;
  }

  const appealPrefix = "/api/ban-appeals/";
  if (req.method === "GET" && pathname.startsWith(appealPrefix)) {
    const decodedUsername = safeDecodeURIComponent(pathname.slice(appealPrefix.length));
    if (decodedUsername === null) {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid user identifier.")
      });
      return;
    }
    const username = normalizeUsername(decodedUsername);
    if (!isValidUsername(username) || !users.has(username)) {
      sendJson(res, 404, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Account not found.")
      });
      return;
    }
    sendJson(res, 200, {
      isBanned: isBanned(username),
      ban: formatBanRecord(getBanRecord(username)),
      request: formatUnbanRequest(unbanRequests.get(username) || null),
      announcement: getLiveSiteAnnouncement()
    });
    return;
  }

  if (req.method === "GET" && pathname === "/api/me") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.")
      });
      return;
    }
    sendJson(res, 200, {
      username,
      isSupervisor: isSupervisor(username),
      isGuest: guestUsers.has(username),
      banned: isBanned(username),
      ban: formatBanRecord(getBanRecord(username)),
      unbanRequest: formatUnbanRequest(unbanRequests.get(username) || null),
      announcement: getLiveSiteAnnouncement()
    });
    return;
  }

  if (req.method === "GET" && pathname === "/api/admin/ban-appeals") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const list = Array.from(unbanRequests.values())
      .sort((a, b) => b.updatedAt - a.updatedAt)
      .map((entry) => ({
        ...formatUnbanRequest(entry),
        isBanned: isBanned(entry.username),
        ban: formatBanRecord(getBanRecord(entry.username))
      }));
    sendJson(res, 200, { requests: list });
    return;
  }

  if (req.method === "GET" && pathname === "/api/admin/users") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const list = Array.from(users.keys())
      .map((item) => formatAdminUserSummary(item))
      .filter(Boolean)
      .sort((a, b) => {
        if (a.isOnline !== b.isOnline) {
          return a.isOnline ? -1 : 1;
        }
        if (a.isBanned !== b.isBanned) {
          return a.isBanned ? -1 : 1;
        }
        if (a.createdAt !== b.createdAt) {
          return b.createdAt - a.createdAt;
        }
        return a.username.localeCompare(b.username);
      });
    sendJson(res, 200, { users: list });
    return;
  }

  if (req.method === "GET" && pathname === "/api/admin/site-presence") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "Unauthorized.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const limit = clampSitePresenceLimit(fullUrl.searchParams.get("limit"));
    const events = await listSitePresenceEvents(limit);
    sendJson(res, 200, { events, limit });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/site-announcement") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION)) {
      return;
    }
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { text } = await parseBody(req);
    const cleanText = String(text || "").trim();
    if (!cleanText) {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Announcement text is required.")
      });
      return;
    }
    if (cleanText.length > 500) {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ (ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 500 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½).", "Announcement is too long (max 500 chars).")
      });
      return;
    }
    const now = Date.now();
    siteAnnouncement = {
      id: `${now}-${crypto.randomBytes(4).toString("hex")}`,
      text: cleanText,
      createdBy: username,
      createdAt: now
    };
    saveModeration();
    recordAudit("admin_site_announcement", username, "site", {
      textLength: cleanText.length
    });
    sendJson(res, 201, { ok: true, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/ban-user") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION)) {
      return;
    }
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, reason } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanReason = String(reason || "").trim();
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Account not found.") });
      return;
    }
    if (isSupervisor(target)) {
      sendJson(res, 400, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Supervisor account cannot be banned.") });
      return;
    }
    if (cleanReason.length < 3 || cleanReason.length > 300) {
      sendJson(res, 400, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 3 ط£آ¯ط·ع؛ط¢آ½ 300 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Ban reason must be 3-300 characters.") });
      return;
    }
    bannedUsers.set(target, {
      username: target,
      reason: cleanReason,
      bannedBy: username,
      bannedAt: Date.now()
    });
    removeUserFromAllRooms(target);
    saveModeration();
    recordAudit("admin_ban_user", username, target, {
      reasonLength: cleanReason.length
    });
    sendJson(res, 200, { ok: true, ban: formatBanRecord(getBanRecord(target)) });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/unban-user") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION)) {
      return;
    }
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, note } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanNote = String(note || "").trim();
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Account not found.") });
      return;
    }
    bannedUsers.delete(target);
    const request = unbanRequests.get(target);
    if (request) {
      request.status = "approved";
      request.reviewedBy = username;
      request.reviewNote = cleanNote;
      request.updatedAt = Date.now();
      unbanRequests.set(target, request);
    }
    saveModeration();
    recordAudit("admin_unban_user", username, target, {
      noteLength: cleanNote.length
    });
    sendJson(res, 200, { ok: true, username: target });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/delete-user") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION)) {
      return;
    }
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Account not found.") });
      return;
    }
    if (isSupervisor(target)) {
      sendJson(res, 400, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Supervisor account cannot be deleted.") });
      return;
    }

    removeUserFromAllRooms(target);
    removeUserSessions(target);
    users.delete(target);
    bannedUsers.delete(target);
    unbanRequests.delete(target);
    userLastSeen.delete(target);
    saveUsers();
    saveModeration();
    recordAudit("admin_delete_user", username, target, {});
    sendJson(res, 200, { ok: true, username: target });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/ban-appeals/decision") {
    if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION)) {
      return;
    }
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, action, note } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanAction = String(action || "").trim().toLowerCase();
    const cleanNote = String(note || "").trim();
    if (!isValidUsername(target) || !unbanRequests.has(target)) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Request not found.") });
      return;
    }
    if (cleanAction !== "approve" && cleanAction !== "reject") {
      sendJson(res, 400, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid action.") });
      return;
    }
    const request = unbanRequests.get(target);
    request.status = cleanAction === "approve" ? "approved" : "rejected";
    request.reviewedBy = username;
    request.reviewNote = cleanNote;
    request.updatedAt = Date.now();
    unbanRequests.set(target, request);
    if (cleanAction === "approve") {
      bannedUsers.delete(target);
    }
    saveModeration();
    recordAudit("admin_ban_appeal_decision", username, target, {
      action: cleanAction,
      noteLength: cleanNote.length
    });
    sendJson(res, 200, { ok: true, request: formatUnbanRequest(request) });
    return;
  }

  if (req.method === "GET" && pathname === "/api/profile") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
    updateUserCountryFromRequest(username, req, { persistRegistered: true });
    const profile = publicProfile(username);
    sendJson(res, 200, {
      profile,
      isSelf: true,
      isSupervisor: isSupervisor(username),
      moderation: {
        isBanned: isBanned(username),
        ban: formatBanRecord(getBanRecord(username)),
        request: formatUnbanRequest(unbanRequests.get(username) || null)
      }
    });
    return;
  }

  if (req.method === "PATCH" && pathname === "/api/profile") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
    const isRegistered = users.has(username);
    const isGuest = guestUsers.has(username);
    if (!isRegistered && !isGuest) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Account not found.") });
      return;
    }
    const { displayName, avatarDataUrl } = await parseBody(req);
    const user = isRegistered ? users.get(username) : guestUsers.get(username);
    const previousRegisteredUser = isRegistered ? { ...user } : null;

    if (displayName !== undefined) {
      const cleanName = String(displayName || "").trim();
      if (cleanName.length < 2 || cleanName.length > 30) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 2 ط£آ¯ط·ع؛ط¢آ½ 30 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Display name must be 2 to 30 characters.")
        });
        return;
      }
      user.displayName = cleanName;
    }

    if (avatarDataUrl !== undefined) {
      const rawAvatar = String(avatarDataUrl || "");
      if (!rawAvatar) {
        user.avatarDataUrl = "";
      } else {
        const isAllowed = /^data:image\/(png|jpe?g|webp);base64,/i.test(rawAvatar);
        if (!isAllowed || rawAvatar.length > 300000) {
          sendJson(res, 400, {
            error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Avatar is invalid or too large.")
          });
          return;
        }
        user.avatarDataUrl = rawAvatar;
      }
    }

    if (isRegistered) {
      users.set(username, user);
      if (!saveUsers()) {
        users.set(username, previousRegisteredUser);
        sendJson(res, 500, {
          error: i18n(
            req,
            "Failed to persist profile changes. Please try again.",
            "Failed to persist profile changes. Please try again."
          )
        });
        return;
      }
    } else {
      guestUsers.set(username, user);
    }
    sendJson(res, 200, { profile: publicProfile(username), isSelf: true });
    return;
  }

  const userProfilePrefix = "/api/users/";
  const userProfileSuffix = "/profile";
  if (
    req.method === "GET" &&
    pathname.startsWith(userProfilePrefix) &&
    pathname.endsWith(userProfileSuffix)
  ) {
    const viewer = getUserFromRequest(req);
    if (!viewer) {
      sendJson(res, 401, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, viewer)) {
      return;
    }
    const rawTarget = pathname.slice(userProfilePrefix.length, pathname.length - userProfileSuffix.length);
    const decodedTarget = safeDecodeURIComponent(String(rawTarget || ""));
    if (decodedTarget === null) {
      sendJson(res, 400, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid user identifier.") });
      return;
    }
    const target = normalizeUsername(decodedTarget);
    if (!isValidUsername(target)) {
      sendJson(res, 400, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid user identifier.") });
      return;
    }
    if (!users.has(target) && !guestUsers.has(target)) {
      sendJson(res, 404, { error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "User not found.") });
      return;
    }
    if (viewer === target) {
      updateUserCountryFromRequest(viewer, req, { persistRegistered: true });
    }
    const allowModerationInfo = isSupervisor(viewer) || viewer === target;
    sendJson(res, 200, {
      profile: publicProfile(target),
      isSelf: viewer === target,
      moderation: allowModerationInfo ? {
        isBanned: isBanned(target),
        ban: formatBanRecord(getBanRecord(target)),
        request: formatUnbanRequest(unbanRequests.get(target) || null)
      } : null
    });
    return;
  }

  if (req.method === "POST" && pathname === "/api/rooms/create") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
    if (!enforceRateLimit(req, res, RATE_LIMITS.ROOMS_CREATE, username)) {
      return;
    }

    const { roomName } = await parseBody(req);
    const cleanRoomName = String(roomName || "").trim();
    if (cleanRoomName.length > ROOM_NAME_MAX_LENGTH) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ (ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 40 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½).",
          "Room name is too long (max 40 characters)."
        )
      });
      return;
    }
    let code = randomRoomCode();
    while (rooms.has(code)) {
      code = randomRoomCode();
    }

    const room = {
      code,
      name: cleanRoomName || i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½", "New Room"),
      host: username,
      pendingHostRestore: null,
      members: new Set([username]),
      approvedUsers: new Set([username]),
      joinRequests: new Set(),
      messageFloorByUser: new Map([[username, 0]]),
      clientMessageLog: new Map(),
      messages: [],
      nextMessageId: 1,
      createdAt: Date.now(),
      video: null,
      videoSync: null,
      videoQueue: [],
      videoHistory: []
    };
    pushSystemMessage(room, "room_created", { user: username });
    rooms.set(code, room);
    recordAudit("room_created", username, code, {
      roomName: room.name
    });
    sendJson(res, 201, { room: formatRoom(room, username) });
    return;
  }

  if (req.method === "GET" && pathname === "/api/rooms") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
    const list = Array.from(rooms.values())
      .sort((a, b) => b.createdAt - a.createdAt)
      .map((room) => formatRoomSummary(room, username));
    sendJson(res, 200, {
      rooms: list,
      stats: {
        onlineUsers: countOnlineUsers(),
        usersInRooms: countUsersInsideRooms()
      },
      announcement: getLiveSiteAnnouncement()
    });
    return;
  }

  if (req.method === "POST" && pathname === "/api/rooms/join") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }

    const { code } = await parseBody(req);
    const roomCode = String(code || "").trim().toUpperCase();
    if (!enforceRateLimit(req, res, RATE_LIMITS.ROOMS_JOIN, roomCode || "unknown")) {
      return;
    }
    if (!roomCode || !rooms.has(roomCode)) {
      sendJson(res, 404, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Room not found.")
      });
      return;
    }

    const room = rooms.get(roomCode);
    ensureRoomRuntimeState(room);

    if (!room.members.has(username)) {
      if (room.approvedUsers.has(username) || isSupervisor(username)) {
        joinUserToRoom(room, username);
        recordAudit("room_joined", username, room.code, {
          via: "join_endpoint"
        });
      } else if (room.joinRequests.has(username)) {
        sendJson(res, 403, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Your join request is still pending.")
        });
        return;
      } else {
        sendJson(res, 403, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "You cannot join until the leader approves your request.")
        });
        return;
      }
    }
    recordAudit("room_opened", username, room.code, {});
    sendJson(res, 200, { room: formatRoom(room, username) });
    return;
  }

  const roomPath = parseRoomPath(pathname);
  if (roomPath) {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }

    if (!rooms.has(roomPath.code)) {
      sendJson(res, 404, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Room not found.")
      });
      return;
    }
    const room = rooms.get(roomPath.code);
    ensureRoomRuntimeState(room);
    if (req.method === "POST" && roomPath.action === "request-join") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOMS_REQUEST_JOIN, room.code)) {
        return;
      }
      const payload = await parseBody(req);
      const cleanAction = String(payload?.action || "").trim().toLowerCase();
      if (cleanAction === "cancel") {
        room.joinRequests.delete(username);
        scheduleRuntimeSnapshotPersistence();
        recordAudit("room_join_request_cancelled", username, room.code, {});
        sendJson(res, 200, {
          ok: true,
          status: "cancelled"
        });
        return;
      }
      if (room.members.has(username)) {
        sendJson(res, 200, {
          ok: true,
          status: "already_member",
          room: formatRoom(room, username)
        });
        return;
      }
      if (room.approvedUsers.has(username)) {
        if (isSupervisor(username)) {
          if (room.joinRequests.has(username)) {
            sendJson(res, 200, {
              ok: true,
              status: "pending"
            });
            return;
          }
          room.joinRequests.add(username);
          scheduleRuntimeSnapshotPersistence();
          recordAudit("room_join_requested", username, room.code, {
            via: "supervisor_pending"
          });
          sendJson(res, 202, {
            ok: true,
            status: "pending"
          });
          return;
        }
        joinUserToRoom(room, username);
        recordAudit("room_joined", username, room.code, {
          via: "request_join_approved"
        });
        sendJson(res, 200, {
          ok: true,
          status: "already_member",
          room: formatRoom(room, username)
        });
        return;
      }
      if (room.joinRequests.has(username)) {
        sendJson(res, 200, {
          ok: true,
          status: "pending"
        });
        return;
      }
      room.joinRequests.add(username);
      scheduleRuntimeSnapshotPersistence();
      recordAudit("room_join_requested", username, room.code, {});
      sendJson(res, 202, {
        ok: true,
        status: "pending"
      });
      return;
    }

    if (!room.members.has(username)) {
      sendJson(res, 403, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Join this room first.")
      });
      return;
    }

    if (req.method === "POST" && roomPath.action === "leave") {
      const result = removeMemberFromRoom(room, username);
      recordAudit("room_left", username, room.code, {});
      sendJson(res, 200, {
        ok: true,
        deleted: result.deleted,
        room: result.room ? formatRoom(result.room, username) : null
      });
      return;
    }

    if (req.method === "GET" && roomPath.action === null) {
      sendJson(res, 200, { room: formatRoom(room, username) });
      return;
    }

    if (req.method === "GET" && roomPath.action === "messages") {
      const rawSince = Number(fullUrl.searchParams.get("since"));
      const since = Number.isFinite(rawSince) && rawSince > 0 ? Math.floor(rawSince) : 0;
      const fromId = since;
      const messages = room.messages.filter((msg) => msg.id > fromId);
      sendJson(res, 200, { room: formatRoom(room, username), messages, announcement: getLiveSiteAnnouncement() });
      return;
    }

    if (req.method === "GET" && roomPath.action === "requests") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Only the room leader can view requests.")
        });
        return;
      }
      sendJson(res, 200, { requests: Array.from(room.joinRequests) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "requests") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOMS_REQUEST_JOIN, room.code)) {
        return;
      }
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Only the room leader can manage requests.")
        });
        return;
      }
      const { username: targetRaw, action } = await parseBody(req);
      const target = normalizeUsername(targetRaw);
      const cleanAction = String(action || "").trim().toLowerCase();
      if (!isValidUsername(target)) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Target username is required.")
        });
        return;
      }
      if (!room.joinRequests.has(target)) {
        sendJson(res, 404, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "No join request found for this player.")
        });
        return;
      }
      if (cleanAction !== "approve" && cleanAction !== "reject") {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid action.")
        });
        return;
      }

      room.joinRequests.delete(target);
      scheduleRuntimeSnapshotPersistence();
      if (cleanAction === "approve") {
        room.approvedUsers.add(target);
        room.messageFloorByUser.set(target, 0);
        const wasMember = room.members.has(target);
        room.members.add(target);
        if (!wasMember) {
          pushSystemMessage(room, "room_joined", { user: target });
        }
        recordAudit("room_join_request_approved", username, room.code, {
          target
        });
        sendJson(res, 200, { ok: true, status: "approved", room: formatRoom(room, username) });
        return;
      }
      if (cleanAction === "reject") {
        recordAudit("room_join_request_rejected", username, room.code, {
          target
        });
        sendJson(res, 200, { ok: true, status: "rejected", room: formatRoom(room, username) });
        return;
      }
    }

    if (req.method === "POST" && roomPath.action === "messages") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOM_MESSAGE_POST, room.code)) {
        return;
      }
      const {
        text,
        clientMessageId,
        replyToMessageId,
        action,
        messageId,
        emoji
      } = await parseBody(req);
      const cleanAction = String(action || "").trim().toLowerCase();
      if (cleanAction === "react") {
        const targetMessageId = normalizeReplyToMessageId(messageId);
        const reactionEmoji = normalizeReactionEmoji(emoji);
        if (Number.isNaN(targetMessageId) || !targetMessageId || !reactionEmoji) {
          sendJson(res, 400, {
            error: i18n(req, "ط¨ظٹط§ظ†ط§طھ ط§ظ„طھظپط§ط¹ظ„ ط؛ظٹط± طµط§ظ„ط­ط©.", "Invalid reaction payload.")
          });
          return;
        }
        const targetMessage = findUserMessageById(room, targetMessageId);
        if (!targetMessage) {
          sendJson(res, 404, {
            error: i18n(req, "ظ„ظ… ظٹطھظ… ط§ظ„ط¹ط«ظˆط± ط¹ظ„ظ‰ ط±ط³ط§ظ„ط© ظ…ظ†ط§ط³ط¨ط© ظ„ظ„طھظپط§ط¹ظ„.", "No eligible message was found for reaction.")
          });
          return;
        }
        const reactions = normalizeMessageReactions(targetMessage.reactions);
        const users = new Set(Array.isArray(reactions[reactionEmoji]) ? reactions[reactionEmoji] : []);
        if (users.has(username)) {
          users.delete(username);
        } else {
          users.add(username);
        }
        if (users.size > 0) {
          reactions[reactionEmoji] = Array.from(users).sort((a, b) => a.localeCompare(b));
        } else {
          delete reactions[reactionEmoji];
        }
        // Keep reaction payload bounded and predictable.
        const cleanReactionEntries = Object.entries(reactions)
          .slice(0, CHAT_REACTION_MAX_TYPES_PER_MESSAGE)
          .reduce((acc, [entryEmoji, entryUsers]) => {
            if (!Array.isArray(entryUsers) || entryUsers.length === 0) {
              return acc;
            }
            acc[entryEmoji] = entryUsers.slice(0, 80);
            return acc;
          }, {});
        targetMessage.reactions = cleanReactionEntries;
        pushSystemMessage(room, "message_reaction", {
          messageId: targetMessage.id,
          reactions: cleanReactionEntries
        });
        sendJson(res, 200, {
          ok: true,
          messageId: targetMessage.id,
          reactions: cleanReactionEntries
        });
        return;
      }
      const messageText = String(text || "").trim();
      if (!messageText) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Message is empty.")
        });
        return;
      }
      if (messageText.length > 300) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Message is too long.")
        });
        return;
      }

      const normalizedReplyToMessageId = normalizeReplyToMessageId(replyToMessageId);
      if (Number.isNaN(normalizedReplyToMessageId)) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid reply identifier.")
        });
        return;
      }
      let replyTo = null;
      if (normalizedReplyToMessageId) {
        replyTo = buildReplyReference(room, normalizedReplyToMessageId);
        if (!replyTo) {
          sendJson(res, 400, {
            error: i18n(
              req,
              "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.",
              "You can only reply to an existing player message."
            )
          });
          return;
        }
      }

      const normalizedClientMessageId = normalizeClientMessageId(clientMessageId);
      if (clientMessageId !== undefined && !normalizedClientMessageId) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid message identifier.")
        });
        return;
      }

      let dedupeKey = "";
      if (normalizedClientMessageId) {
        dedupeKey = `${username}:${normalizedClientMessageId}`;
        pruneClientMessageLog(room);
        const existing = room.clientMessageLog.get(dedupeKey);
        if (existing) {
          sendJson(res, 200, { ok: true, duplicate: true, messageId: existing.messageId });
          return;
        }
      }

      const createdMessageId = pushUserMessage(room, username, messageText, replyTo);
      if (dedupeKey) {
        room.clientMessageLog.set(dedupeKey, {
          messageId: createdMessageId,
          timestamp: Date.now()
        });
      }
      sendJson(res, 201, { ok: true, messageId: createdMessageId });
      return;
    }

    if (req.method === "DELETE" && roomPath.action === "video") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOM_VIDEO_SOURCE, room.code)) {
        return;
      }
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½ط·آ£ط¢آ¯ط·آ·ط¹ط›ط·آ¢ط¢آ½.", "Only the room leader can remove videos.")
        });
        return;
      }
      const hadVideo = Boolean(room.video && room.video.src);
      removeRoomVideoAsset(room);
      if (hadVideo) {
        pushSystemMessage(room, "room_video_removed", { user: username });
      }
      broadcastVideoSync(room);
      recordAudit("room_video_removed", username, room.code, {
        hadVideo
      });
      sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video") {
      const isRoomLeader = room.host === username;
      const contentType = String(req.headers["content-type"] || "").toLowerCase();
      if (contentType.includes("application/json")) {
        if (!isRoomLeader) {
          sendJson(res, 403, {
            code: "VIDEO_HOST_ONLY",
            error: i18n(req, "Only the room leader can remove videos.", "Only the room leader can remove videos.")
          });
          return;
        }
        if (!enforceRateLimit(req, res, RATE_LIMITS.ROOM_VIDEO_SOURCE, room.code)) {
          return;
        }
        const { action } = await parseBody(req);
        const clearAction = String(action || "").trim().toLowerCase();
        if (clearAction === "clear" || clearAction === "remove" || clearAction === "delete") {
          const hadVideo = Boolean(room.video && room.video.src);
          removeRoomVideoAsset(room);
          if (hadVideo) {
            pushSystemMessage(room, "room_video_removed", { user: username });
          }
          broadcastVideoSync(room);
          recordAudit("room_video_removed", username, room.code, {
            hadVideo,
            via: "video_post"
          });
          sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
          return;
        }
        sendJson(res, 400, {
          code: "VIDEO_INVALID_ACTION",
          error: i18n(req, "Invalid video action.", "Invalid video action.")
        });
        return;
      }
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOM_VIDEO_UPLOAD, room.code)) {
        return;
      }

      if (!ensureUploadsDir()) {
        sendJson(res, 503, {
          code: "VIDEO_STORAGE_UNAVAILABLE",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Server storage is unavailable right now.")
        });
        req.resume();
        return;
      }
      if (typeof req.setTimeout === "function") {
        req.setTimeout(0);
      }

      const multipart = await parseMultipartFormData(req, ROOM_VIDEO_MAX_BYTES);
      const uploaded = multipart.files.find((file) => file.fieldName === "video") || multipart.files[0] || null;
      const cleanupUploadedTemp = () => {
        if (uploaded?.tempPath) {
          removeFileIfExists(uploaded.tempPath);
        }
      };
      if (!uploaded || !uploaded.tempPath || Number(uploaded.size || 0) <= 0) {
        cleanupUploadedTemp();
        sendJson(res, 400, {
          code: "VIDEO_FILE_REQUIRED",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Please choose a valid video file.")
        });
        return;
      }
      if (uploaded.truncated || Number(uploaded.size || 0) > ROOM_VIDEO_MAX_BYTES) {
        cleanupUploadedTemp();
        sendJson(res, 413, {
          code: "VIDEO_TOO_LARGE",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ (ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ 1GB).", "Video file is too large (max 1GB).")
        });
        return;
      }

      const mimeType = normalizeVideoMimeType(uploaded.contentType);
      if (!ROOM_VIDEO_ALLOWED_MIME_TYPES.has(mimeType)) {
        cleanupUploadedTemp();
        sendJson(res, 415, {
          code: "VIDEO_INVALID_TYPE",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½. ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ MP4 ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ WebM ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ OGG.", "Unsupported video type. Use MP4, WebM or OGG.")
        });
        return;
      }
      const cleanFileName = sanitizeUploadedFilename(uploaded.filename || "room-video");
      const normalizedDuration = normalizeRoomVideoDuration(multipart.fields.duration);
      const enqueueRequested = resolveRoomVideoEnqueueMode(
        room,
        username,
        multipart.fields.enqueue,
        multipart.fields.mode
      );
      let videoEntry = null;
      let persistedBlobId = "";
      if (isSqlRuntimeEnabled()) {
        let blob = null;
        try {
          blob = await persistRoomVideoBlobFromFileToSql(uploaded.tempPath, {
            mimeType,
            sourceKind: "upload",
            fileName: cleanFileName
          });
        } finally {
          cleanupUploadedTemp();
        }
        if (!blob?.src) {
          sendJson(res, 503, {
            code: "VIDEO_STORAGE_UNAVAILABLE",
            error: i18n(req, "Server storage is unavailable right now.", "Server storage is unavailable right now.")
          });
          return;
        }
        persistedBlobId = String(blob.blobId || "");
        videoEntry = createRoomFileVideoEntry(room, {
          src: blob.src,
          filename: cleanFileName,
          mimeType,
          size: Number(blob.size || uploaded.size || 0),
          uploadedBy: username,
          duration: normalizedDuration
        });
      } else {
        const extension = normalizeVideoExtension(cleanFileName, mimeType);
        const storedFileName = `room-${room.code.toLowerCase()}-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${extension}`;
        const absolutePath = path.join(getRoomVideoUploadDir(), storedFileName);
        try {
          fs.renameSync(uploaded.tempPath, absolutePath);
        } catch (_error) {
          cleanupUploadedTemp();
          sendJson(res, 503, {
            code: "VIDEO_STORAGE_UNAVAILABLE",
            error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Server storage is unavailable right now.")
          });
          return;
        }
        videoEntry = createRoomFileVideoEntry(room, {
          storedFileName,
          filename: cleanFileName,
          mimeType,
          size: Number(uploaded.size || 0),
          uploadedBy: username,
          duration: normalizedDuration
        });
      }
      if (!videoEntry) {
        if (persistedBlobId) {
          deleteRoomVideoBlobFromSql(persistedBlobId).catch(() => undefined);
        }
        sendJson(res, 500, {
          code: "VIDEO_STORAGE_UNAVAILABLE",
          error: i18n(req, "Server storage is unavailable right now.", "Server storage is unavailable right now.")
        });
        return;
      }
      const applyResult = setOrQueueRoomVideo(room, videoEntry, { enqueue: enqueueRequested });
      const queueLength = Array.isArray(room.videoQueue) ? room.videoQueue.length : 0;
      if (applyResult.queued) {
        recordAudit("room_video_queued", username, room.code, {
          fileName: cleanFileName,
          queueLength
        });
        sendJson(res, 201, {
          ok: true,
          queued: true,
          queueLength,
          room: formatRoom(room, username),
          video: formatRoomVideo(room),
          queuedVideo: formatRoomVideoEntry(applyResult.queuedEntry, room)
        });
        return;
      }
      pushSystemMessage(room, "room_video_set", {
        user: username,
        name: String(room.video?.filename || "video")
      });
      recordAudit("room_video_set", username, room.code, {
        fileName: cleanFileName,
        queueLength
      });
      broadcastVideoSync(room);
      sendJson(res, 201, {
        ok: true,
        queued: false,
        queueLength,
        room: formatRoom(room, username),
        video: formatRoomVideo(room)
      });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-source") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOM_VIDEO_SOURCE, room.code)) {
        return;
      }

      const { input, enqueue, mode } = await parseBody(req);
      const rawInput = String(input || "").trim();
      const enqueueRequested = resolveRoomVideoEnqueueMode(room, username, enqueue, mode);
      if (rawInput.length < 2) {
        sendJson(res, 400, {
          code: "YOUTUBE_INPUT_REQUIRED",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Enter a YouTube URL or search text.")
        });
        return;
      }

      let resolved = null;
      try {
        resolved = await resolveYouTubeInput(rawInput);
      } catch (_error) {
        sendJson(res, 502, {
          code: "YOUTUBE_RESOLVE_FAILED",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½. ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Could not reach YouTube right now. Try again.")
        });
        return;
      }

      if (!resolved?.videoId) {
        sendJson(res, 404, {
          code: "YOUTUBE_NOT_FOUND",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "No matching YouTube video was found.")
        });
        return;
      }
      let videoEntry = null;
      let downloadedLabel = resolved.label;
      if (isSqlRuntimeEnabled()) {
        let cachedBlob = null;
        try {
          cachedBlob = await fetchLatestYouTubeRoomVideoBlobFromSql(resolved.videoId);
        } catch (error) {
          console.error("Failed to read cached YouTube SQL blob:", error.message);
        }
        if (cachedBlob?.src) {
          downloadedLabel = cachedBlob.fileName || downloadedLabel;
          videoEntry = createRoomFileVideoEntry(room, {
            src: cachedBlob.src,
            filename: cachedBlob.fileName || resolved.label || `youtube-${resolved.videoId}`,
            mimeType: cachedBlob.mimeType,
            size: Number(cachedBlob.size || 0),
            uploadedBy: username,
            youtubeId: resolved.videoId
          });
        }
        if (!videoEntry) {
          if (!ensureUploadsDir()) {
            sendJson(res, 503, {
              code: "VIDEO_STORAGE_UNAVAILABLE",
              error: i18n(req, "Server storage is unavailable right now.", "Server storage is unavailable right now.")
            });
            return;
          }
          let downloaded = null;
          let downloadedAbsolutePath = "";
          let persistedBlobId = "";
          try {
            downloaded = await downloadYouTubeVideoToLocalFile(resolved.videoId);
            downloadedAbsolutePath = path.join(getRoomVideoUploadDir(), downloaded.storedFileName);
            downloadedLabel = downloaded.cleanFileName || downloadedLabel;
            const blob = await persistRoomVideoBlobFromFileToSql(downloadedAbsolutePath, {
              mimeType: downloaded.mimeType,
              sourceKind: "youtube",
              fileName: downloaded.cleanFileName,
              youtubeId: resolved.videoId
            });
            if (!blob?.src) {
              throw new Error("SQL_VIDEO_STORE_FAILED");
            }
            persistedBlobId = String(blob.blobId || "");
            videoEntry = createRoomFileVideoEntry(room, {
              src: blob.src,
              filename: downloaded.cleanFileName || resolved.label || `youtube-${resolved.videoId}`,
              mimeType: downloaded.mimeType,
              size: Number(blob.size || downloaded.downloadedBytes || 0),
              uploadedBy: username,
              duration: downloaded.durationSec,
              youtubeId: resolved.videoId
            });
            if (!videoEntry && persistedBlobId) {
              await deleteRoomVideoBlobFromSql(persistedBlobId).catch(() => undefined);
            }
          } catch (error) {
            const errorCode = String(error?.code || "").trim().toUpperCase();
            if (persistedBlobId) {
              deleteRoomVideoBlobFromSql(persistedBlobId).catch(() => undefined);
            }
            if (errorCode === "YOUTUBE_VIDEO_TOO_LARGE") {
              sendJson(res, 413, {
                code: "VIDEO_TOO_LARGE",
                error: i18n(req, "Video file is too large (max 1GB).", "Video file is too large (max 1GB).")
              });
              return;
            }
            try {
              const directPlayable = await getYouTubeDirectPlayableSource(resolved.videoId);
              const proxy = registerYouTubeProxyStream(resolved.videoId, directPlayable);
              const proxyEntry = createRoomFileVideoEntry(room, {
                src: proxy.src,
                filename: directPlayable.cleanFileName || resolved.label || `youtube-${resolved.videoId}`,
                mimeType: directPlayable.mimeType,
                size: Number(directPlayable.estimatedBytes || 0),
                uploadedBy: username,
                duration: directPlayable.durationSec,
                youtubeId: resolved.videoId
              });
              if (proxyEntry) {
                proxyEntry.isYouTubeProxy = true;
                videoEntry = proxyEntry;
                downloadedLabel = directPlayable.cleanFileName || downloadedLabel;
              }
            } catch (_proxyError) {
              // Keep the original failure path when direct proxy source also fails.
            }
            if (!videoEntry) {
              // Final fallback: start classic YouTube playback instead of failing the request.
              videoEntry = createYouTubeEmbedVideoEntry(resolved.videoId, username, resolved.label);
              downloadedLabel = resolved.label || downloadedLabel;
            }
            if (!videoEntry) {
              sendJson(res, 502, {
                code: "YOUTUBE_DOWNLOAD_FAILED",
                error: i18n(
                  req,
                  "Could not download this YouTube video right now. Try another one.",
                  "Could not download this YouTube video right now. Try another one."
                )
              });
              return;
            }
          } finally {
            if (downloadedAbsolutePath) {
              removeFileIfExists(downloadedAbsolutePath);
            }
          }
        }
      } else {
        try {
          const directPlayable = await getYouTubeDirectPlayableSource(resolved.videoId);
          const proxy = registerYouTubeProxyStream(resolved.videoId, directPlayable);
          const proxyEntry = createRoomFileVideoEntry(room, {
            src: proxy.src,
            filename: directPlayable.cleanFileName || resolved.label || `youtube-${resolved.videoId}`,
            mimeType: directPlayable.mimeType,
            size: Number(directPlayable.estimatedBytes || 0),
            uploadedBy: username,
            duration: directPlayable.durationSec,
            youtubeId: resolved.videoId
          });
          if (proxyEntry) {
            proxyEntry.isYouTubeProxy = true;
            videoEntry = proxyEntry;
            downloadedLabel = directPlayable.cleanFileName || downloadedLabel;
          }
        } catch (_proxyError) {
          videoEntry = createYouTubeEmbedVideoEntry(resolved.videoId, username, resolved.label);
          downloadedLabel = resolved.label || downloadedLabel;
        }
      }
      if (!videoEntry) {
        sendJson(res, 400, {
          code: "YOUTUBE_INVALID_VIDEO",
          error: i18n(req, "Invalid video action.", "Invalid video action.")
        });
        return;
      }
      const applyResult = setOrQueueRoomVideo(room, videoEntry, { enqueue: enqueueRequested });
      const queueLength = Array.isArray(room.videoQueue) ? room.videoQueue.length : 0;
      if (applyResult.queued) {
        recordAudit("room_video_queued", username, room.code, {
          source: "youtube",
          youtubeId: resolved.videoId,
          queueLength
        });
        sendJson(res, 201, {
          ok: true,
          queued: true,
          queueLength,
          room: formatRoom(room, username),
          video: formatRoomVideo(room),
          queuedVideo: formatRoomVideoEntry(applyResult.queuedEntry, room)
        });
        return;
      }
      pushSystemMessage(room, "room_video_set", {
        user: username,
        name: String(room.video?.filename || downloadedLabel || resolved.label || "video")
      });
      recordAudit("room_video_set", username, room.code, {
        source: "youtube",
        youtubeId: resolved.videoId,
        queueLength
      });
      broadcastVideoSync(room);
      sendJson(res, 201, {
        ok: true,
        queued: false,
        queueLength,
        room: formatRoom(room, username),
        video: formatRoomVideo(room)
      });
      return;
    }

    if (req.method === "GET" && roomPath.action === "video-queue") {
      sendJson(res, 200, {
        ok: true,
        room: formatRoom(room, username),
        video: formatRoomVideo(room),
        queue: formatRoomVideoQueue(room),
        queueLength: Array.isArray(room.videoQueue) ? room.videoQueue.length : 0,
        historyCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
      });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-queue") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ROOM_VIDEO_SOURCE, room.code)) {
        return;
      }
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "Only the room leader can manage queue.", "Only the room leader can manage queue.")
        });
        return;
      }
      const payload = await parseBody(req);
      const action = String(payload?.action || "").trim().toLowerCase();
      if (!action) {
        sendJson(res, 400, {
          code: "VIDEO_QUEUE_ACTION_REQUIRED",
          error: i18n(req, "Queue action is required.", "Queue action is required.")
        });
        return;
      }
      if (action === "next") {
        const next = playNextQueuedRoomVideo(room);
        if (!next) {
          sendJson(res, 404, {
            code: "VIDEO_QUEUE_EMPTY",
            error: i18n(req, "Queue is empty.", "Queue is empty.")
          });
          return;
        }
        pushSystemMessage(room, "room_video_set", {
          user: username,
          name: String(next.filename || "video")
        });
        recordAudit("room_video_queue_next", username, room.code, {
          queueLength: room.videoQueue.length
        });
        broadcastVideoSync(room);
        sendJson(res, 200, {
          ok: true,
          action,
          room: formatRoom(room, username),
          video: formatRoomVideo(room),
          queue: formatRoomVideoQueue(room),
          queueLength: room.videoQueue.length,
          historyCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
        });
        return;
      }
      if (action === "prev" || action === "previous") {
        const previous = playPreviousRoomVideo(room);
        if (!previous) {
          sendJson(res, 404, {
            code: "VIDEO_HISTORY_EMPTY",
            error: i18n(req, "History is empty.", "History is empty.")
          });
          return;
        }
        pushSystemMessage(room, "room_video_set", {
          user: username,
          name: String(previous.filename || "video")
        });
        recordAudit("room_video_queue_previous", username, room.code, {
          queueLength: room.videoQueue.length
        });
        broadcastVideoSync(room);
        sendJson(res, 200, {
          ok: true,
          action,
          room: formatRoom(room, username),
          video: formatRoomVideo(room),
          queue: formatRoomVideoQueue(room),
          queueLength: room.videoQueue.length,
          historyCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
        });
        return;
      }
      if (action === "remove") {
        const rawIndex = Number(payload?.index ?? payload?.queueIndex);
        const index = Number.isInteger(rawIndex) ? rawIndex : Math.floor(rawIndex);
        const removed = removeQueuedRoomVideoAt(room, index);
        if (!removed) {
          sendJson(res, 404, {
            code: "VIDEO_QUEUE_ITEM_NOT_FOUND",
            error: i18n(req, "Queue item not found.", "Queue item not found.")
          });
          return;
        }
        recordAudit("room_video_queue_removed", username, room.code, {
          index,
          fileName: removed.filename || ""
        });
        broadcastVideoSync(room);
        sendJson(res, 200, {
          ok: true,
          action,
          room: formatRoom(room, username),
          video: formatRoomVideo(room),
          queue: formatRoomVideoQueue(room),
          queueLength: room.videoQueue.length,
          historyCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
        });
        return;
      }
      if (action === "clear") {
        const clearHistory = parseBooleanFlag(payload?.clearHistory);
        clearRoomVideoQueue(room, { clearHistory });
        recordAudit("room_video_queue_cleared", username, room.code, {
          clearHistory
        });
        broadcastVideoSync(room);
        sendJson(res, 200, {
          ok: true,
          action,
          room: formatRoom(room, username),
          video: formatRoomVideo(room),
          queue: formatRoomVideoQueue(room),
          queueLength: room.videoQueue.length,
          historyCount: Array.isArray(room.videoHistory) ? room.videoHistory.length : 0
        });
        return;
      }
      sendJson(res, 400, {
        code: "VIDEO_QUEUE_ACTION_INVALID",
        error: i18n(req, "Invalid action.", "Invalid action.")
      });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-sync") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Only the room leader can sync the video.")
        });
        return;
      }
      if (!room.video) {
        sendJson(res, 404, {
          code: "VIDEO_NOT_FOUND",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "No room video is available.")
        });
        return;
      }
      // Backward-compatible guard: older in-memory rooms might have a YouTube video without sync state.
      if (!room.videoSync) {
        room.videoSync = {
          videoId: String(room.video.id || randomToken()),
          playing: false,
          baseTime: 0,
          playbackRate: 1,
          updatedAt: Date.now(),
          clientNow: 0,
          clientSeq: 0
        };
      }

      const {
        action,
        currentTime,
        playbackRate,
        duration,
        videoId,
        playing,
        heartbeat,
        allowRewind,
        clientNow,
        clientSeq
      } = await parseBody(req);
      const cleanAction = String(action || "").trim().toLowerCase();
      if (!["play", "pause", "seek", "rate", "stop"].includes(cleanAction)) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid video sync action.")
        });
        return;
      }

      const clientVideoId = String(videoId || "").trim();
      if (clientVideoId && clientVideoId !== String(room.video.id || "")) {
        sendJson(res, 409, {
          code: "VIDEO_STALE",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½. ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "This video version is outdated. Refresh room data.")
        });
        return;
      }

      const incomingClientNow = Number(clientNow);
      const incomingClientSeq = Number(clientSeq);
      const normalizedClientNow = Number.isFinite(incomingClientNow) && incomingClientNow > 0
        ? Math.floor(incomingClientNow)
        : 0;
      const normalizedClientSeq = Number.isFinite(incomingClientSeq) && incomingClientSeq > 0
        ? Math.floor(incomingClientSeq)
        : 0;
      const lastClientNow = Number(room.videoSync.clientNow || 0);
      const lastClientSeq = Number(room.videoSync.clientSeq || 0);
      const staleByClientOrder =
        (normalizedClientNow > 0 && lastClientNow > 0 && normalizedClientNow < lastClientNow) ||
        (
          normalizedClientNow > 0 &&
          lastClientNow > 0 &&
          normalizedClientNow === lastClientNow &&
          normalizedClientSeq > 0 &&
          normalizedClientSeq <= lastClientSeq
        );
      if (staleByClientOrder) {
        sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
        return;
      }

      const now = Date.now();
      const effective = getRoomVideoEffectiveTime(room, now);
      const nextRate = normalizeRoomVideoPlaybackRate(
        playbackRate !== undefined ? playbackRate : room.videoSync.playbackRate
      );
      const requestedTime = currentTime !== undefined ? clampRoomVideoTime(room, currentTime) : effective;
      const hasPlaying = playing !== undefined;
      const nextPlaying = Boolean(playing);
      const isHeartbeat = Boolean(heartbeat);
      const canRewind = Boolean(allowRewind);
      let nextTime = requestedTime;
      const actionCanAdvanceTimeline =
        cleanAction === "play" || cleanAction === "pause" || cleanAction === "seek";
      const shouldIgnoreUnexpectedRewind =
        actionCanAdvanceTimeline &&
        !canRewind &&
        requestedTime + ROOM_VIDEO_HEARTBEAT_REWIND_GUARD_SEC < effective;
      const shouldIgnoreHeartbeatRewind =
        cleanAction === "seek" &&
        isHeartbeat &&
        requestedTime + ROOM_VIDEO_HEARTBEAT_REWIND_GUARD_SEC < effective;
      if (shouldIgnoreUnexpectedRewind || shouldIgnoreHeartbeatRewind) {
        nextTime = effective;
      }
      const normalizedDuration = normalizeRoomVideoDuration(duration);
      if (normalizedDuration > 0) {
        room.video.duration = normalizedDuration;
      }

      if (cleanAction === "play") {
        room.videoSync.playing = true;
        room.videoSync.baseTime = nextTime;
        room.videoSync.playbackRate = nextRate;
        room.videoSync.updatedAt = now;
      } else if (cleanAction === "pause") {
        room.videoSync.playing = false;
        room.videoSync.baseTime = nextTime;
        room.videoSync.playbackRate = nextRate;
        room.videoSync.updatedAt = now;
      } else if (cleanAction === "seek") {
        room.videoSync.baseTime = nextTime;
        room.videoSync.updatedAt = now;
        if (hasPlaying) {
          room.videoSync.playing = nextPlaying;
        }
        if (playbackRate !== undefined) {
          room.videoSync.playbackRate = nextRate;
        }
      } else if (cleanAction === "rate") {
        room.videoSync.baseTime = effective;
        if (hasPlaying) {
          room.videoSync.playing = nextPlaying;
        }
        room.videoSync.playbackRate = nextRate;
        room.videoSync.updatedAt = now;
      } else if (cleanAction === "stop") {
        room.videoSync.playing = false;
        room.videoSync.baseTime = 0;
        room.videoSync.playbackRate = 1;
        room.videoSync.updatedAt = now;
      }

      if (normalizedClientNow > 0) {
        room.videoSync.clientNow = normalizedClientNow;
        room.videoSync.clientSeq = normalizedClientSeq > 0
          ? normalizedClientSeq
          : lastClientSeq;
      } else {
        room.videoSync.clientNow = Math.max(lastClientNow, now);
        room.videoSync.clientSeq = normalizedClientSeq > 0
          ? Math.max(lastClientSeq, normalizedClientSeq)
          : lastClientSeq;
      }
      if (!isHeartbeat) {
        scheduleRuntimeSnapshotPersistence();
      }
      broadcastVideoSync(room);
      sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "host") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION, room.code)) {
        return;
      }
      const {
        action,
        username: targetRaw
      } = await parseBody(req);
      const cleanAction = String(action || "transfer").trim().toLowerCase();

      if (cleanAction === "claim") {
        if (!isSupervisor(username) || !room.members.has(username)) {
          sendJson(res, 403, {
            code: "HOST_CLAIM_FORBIDDEN",
            error: i18n(
              req,
              "ظپظ‚ط· ظ…ط´ط±ظپ ظ…ظˆط¬ظˆط¯ ط¯ط§ط®ظ„ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ط§ظ…طھظ„ط§ظƒ ط§ظ„ظ‚ظٹط§ط¯ط©.",
              "Only a supervisor in this room can claim leadership."
            )
          });
          return;
        }
        const previous = room.host;
        room.host = username;
        room.pendingHostRestore = null;
        if (previous !== username) {
          pushSystemMessage(room, "host_changed", { user: username, previous });
        }
        scheduleRuntimeSnapshotPersistence();
        recordAudit("room_host_claimed", username, room.code, {
          previous
        });
        sendJson(res, 200, { ok: true, room: formatRoom(room, username) });
        return;
      }

      if (room.host !== username) {
        sendJson(res, 403, {
          code: "HOST_TRANSFER_FORBIDDEN",
          error: i18n(
            req,
            "ظپظ‚ط· ظ‚ط§ط¦ط¯ ط§ظ„ط؛ط±ظپط© ظٹظ…ظƒظ†ظ‡ ظ†ظ‚ظ„ ط§ظ„ظ‚ظٹط§ط¯ط©.",
            "Only the room leader can transfer leadership."
          )
        });
        return;
      }

      const target = normalizeUsername(targetRaw);
      const targetValid =
        isValidUsername(target) &&
        target !== room.host &&
        room.members.has(target);
      if (!targetValid) {
        sendJson(res, 400, {
          code: "HOST_TRANSFER_TARGET_INVALID",
          error: i18n(
            req,
            "ظٹط¬ط¨ ط§ط®طھظٹط§ط± ط¹ط¶ظˆ ط¢ط®ط± ط¯ط§ط®ظ„ ط§ظ„ط؛ط±ظپط© ظ„ظ†ظ‚ظ„ ط§ظ„ظ‚ظٹط§ط¯ط©.",
            "Leader transfer target must be another member."
          )
        });
        return;
      }

      const previous = room.host;
      room.host = target;
      room.pendingHostRestore = null;
      pushSystemMessage(room, "host_changed", { user: target, previous });
      recordAudit("room_host_transferred", username, room.code, {
        previous,
        target
      });
      sendJson(res, 200, { ok: true, room: formatRoom(room, username) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "kick") {
      if (!enforceRateLimit(req, res, RATE_LIMITS.ADMIN_ACTION, room.code)) {
        return;
      }
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Only the room leader can kick players.")
        });
        return;
      }

      const { username: targetRaw } = await parseBody(req);
      const target = normalizeUsername(targetRaw);
      if (!isValidUsername(target)) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Target username is required.")
        });
        return;
      }
      if (target === room.host) {
        sendJson(res, 400, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Leader cannot kick themselves.")
        });
        return;
      }
      if (isSupervisor(target)) {
        sendJson(res, 403, {
          code: "SUPERVISOR_KICK_FORBIDDEN",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "A supervisor cannot be kicked from this room.")
        });
        return;
      }
      if (!room.members.has(target)) {
        sendJson(res, 404, {
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Player is not in this room.")
        });
        return;
      }

      room.members.delete(target);
      room.approvedUsers.delete(target);
      if (room.pendingHostRestore === target) {
        room.pendingHostRestore = null;
      }
      room.joinRequests.delete(target);
      pushSystemMessage(room, "player_kicked", { user: target });
      disconnectRoomMemberSockets(room.code, target, "kicked");
      scheduleRuntimeSnapshotPersistence();
      broadcastRoomTouch(room, "member_kicked", { username: target });
      recordAudit("room_member_kicked", username, room.code, {
        target
      });
      sendJson(res, 200, { room: formatRoom(room, username) });
      return;
    }

    sendJson(res, 404, {
      error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Room endpoint not found.")
    });
    return;
  }

  sendJson(res, 404, {
    error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Endpoint not found.")
  });
}

const server = http.createServer(async (req, res) => {
  let requestPathname = "";
  try {
    const parsedUrl = new URL(req.url, "http://localhost");
    requestPathname = parsedUrl.pathname;
    if (requestPathname.startsWith("/api/")) {
      await handleApi(req, res);
      return;
    }
    serveStatic(requestPathname, req, res);
  } catch (error) {
    if (error.message === "Payload too large") {
      const roomPath = parseRoomPath(requestPathname);
      const contentType = String(req.headers["content-type"] || "").toLowerCase();
      if (
        req.method === "POST"
        && roomPath?.action === "video"
        && contentType.includes("multipart/form-data")
      ) {
        sendJson(res, 413, {
          code: "VIDEO_TOO_LARGE",
          error: i18n(req, "Video file is too large (max 1GB).", "Video file is too large (max 1GB).")
        });
        return;
      }
      sendJson(res, 413, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Payload is too large.")
      });
      return;
    }
    if (error.message === "Invalid JSON") {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ JSON ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid JSON body.")
      });
      return;
    }
    if (error.message === "Invalid multipart form data") {
      sendJson(res, 400, {
        error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Invalid video upload payload.")
      });
      return;
    }
    sendJson(res, 500, {
      error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Internal server error.")
    });
  }
});

// Large room-video uploads can exceed default Node request timeout.
server.requestTimeout = 0;
server.headersTimeout = 120000;
setupWebSocketServer(server);

async function bootstrap() {
  warnIfUsersStorageIsEphemeral();
  await initSqlRuntime();
  await loadUsers();
  loadModeration();
  ensureUploadsDir();
  loadYouTubeCache();
  if (isSqlRuntimeEnabled()) {
    await loadRuntimeSnapshotFromSql();
    await pruneOrphanedRoomVideoBlobsFromSql();
  }
  server.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
  });
}

bootstrap().catch((error) => {
  console.error("Failed to bootstrap server:", error.message);
  process.exit(1);
});


