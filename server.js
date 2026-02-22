const http = require("http");
const fs = require("fs");
const https = require("https");
const path = require("path");
const crypto = require("crypto");
const ytdl = require("@distube/ytdl-core");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const UPLOADS_DIR = path.join(ROOT, "uploads");
const ROOM_VIDEO_UPLOAD_DIR = path.join(UPLOADS_DIR, "room-videos");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const MODERATION_FILE = path.join(DATA_DIR, "moderation.json");
const SUPERVISOR_USERNAME = "qasim";
const GUEST_USERNAME_PREFIX = "guest_";
const SITE_ANNOUNCEMENT_LIVE_MS = 15000;
const SESSION_TTL_MS = 1000 * 60 * 60 * 24 * 7;
const USERNAME_MIN_LENGTH = 3;
const USERNAME_MAX_LENGTH = 30;
const PASSWORD_MIN_LENGTH = 4;
const PASSWORD_MAX_LENGTH = 128;
const ROOM_NAME_MAX_LENGTH = 40;
const USERNAME_ALLOWED_REGEX = /^[\p{L}\p{N}_-]+$/u;
const CLIENT_MESSAGE_ID_MAX_LENGTH = 80;
const CLIENT_MESSAGE_ID_REGEX = /^[A-Za-z0-9_-]+$/;
const MESSAGE_DEDUP_TTL_MS = 1000 * 60 * 5;
const REPLY_PREVIEW_MAX_LENGTH = 160;
const ROOM_VIDEO_MAX_BYTES = 80 * 1024 * 1024;
const ROOM_VIDEO_MULTIPART_OVERHEAD_BYTES = 1024 * 1024;
const ROOM_VIDEO_MAX_DURATION_SEC = 60 * 60 * 8;
const ROOM_VIDEO_MAX_FILENAME_LENGTH = 120;
const YOUTUBE_SEARCH_TIMEOUT_MS = 9000;
const YOUTUBE_DOWNLOAD_TIMEOUT_MS = 1000 * 60 * 4;
const YOUTUBE_INPUT_MAX_LENGTH = 300;
const YOUTUBE_VIDEO_ID_REGEX = /^[A-Za-z0-9_-]{11}$/;
const ROOM_VIDEO_ALLOWED_MIME_TYPES = new Set([
  "video/mp4",
  "video/webm",
  "video/ogg"
]);
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
const userLastSeen = new Map();
const bannedUsers = new Map();
const unbanRequests = new Map();
let siteAnnouncement = null;

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function ensureUploadsDir() {
  if (!fs.existsSync(ROOM_VIDEO_UPLOAD_DIR)) {
    fs.mkdirSync(ROOM_VIDEO_UPLOAD_DIR, { recursive: true });
  }
}

function loadUsers() {
  try {
    ensureDataDir();
    if (!fs.existsSync(USERS_FILE)) {
      fs.writeFileSync(USERS_FILE, "[]", "utf8");
      return;
    }
    const raw = fs.readFileSync(USERS_FILE, "utf8");
    const list = JSON.parse(raw);
    if (!Array.isArray(list)) {
      return;
    }
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
        avatarDataUrl: typeof item.avatarDataUrl === "string" ? item.avatarDataUrl : ""
      });
    });
  } catch (error) {
    console.error("Failed to load users:", error.message);
  }
}

function saveUsers() {
  try {
    ensureDataDir();
    const list = Array.from(users.entries()).map(([username, info]) => ({
      username,
      passwordHash: info.passwordHash,
      passwordSalt: typeof info.passwordSalt === "string" ? info.passwordSalt : "",
      createdAt: info.createdAt,
      displayName: normalizeDisplayName(info.displayName, username),
      avatarDataUrl: info.avatarDataUrl || ""
    }));
    fs.writeFileSync(USERS_FILE, JSON.stringify(list, null, 2), "utf8");
  } catch (error) {
    console.error("Failed to save users:", error.message);
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
    const parsed = JSON.parse(raw);
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

function getLang(req) {
  return req.headers["x-lang"] === "en" ? "en" : "ar";
}

function looksLikeMojibake(text) {
  if (typeof text !== "string" || text.length < 8) {
    return false;
  }
  const suspicious = text.match(/[طظ]/g);
  return Boolean(suspicious && suspicious.length >= 3 && suspicious.length / text.length >= 0.2);
}

function normalizeDisplayName(value, fallbackUsername = "") {
  const fallback = String(fallbackUsername || "").trim().toLowerCase().slice(0, 30);
  const clean = String(value || "").trim().slice(0, 30);
  if (!clean || looksLikeMojibake(clean)) {
    return fallback || SUPERVISOR_USERNAME;
  }
  return clean;
}

function i18n(req, ar, en) {
  if (getLang(req) === "en") {
    return en;
  }
  return looksLikeMojibake(ar) ? en : ar;
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
  saveUsers();
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
}

function isValidUsername(username) {
  return (
    typeof username === "string" &&
    username.length >= USERNAME_MIN_LENGTH &&
    username.length <= USERNAME_MAX_LENGTH &&
    USERNAME_ALLOWED_REGEX.test(username)
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
      "تم حظر هذا الحساب من الموقع. يمكنك إرسال طلب رفع حظر للمشرف.",
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
    error: i18n(req, "هذه الخاصية للمشرف فقط.", "This action is supervisor-only.")
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
    return null;
  }
  if (normalized.expiresAt && normalized.expiresAt < Date.now()) {
    sessions.delete(token);
    if (normalized.isGuest && !hasActiveSessionForUser(normalized.username, token)) {
      cleanupGuestUser(normalized.username);
    }
    return null;
  }

  normalized.expiresAt = Date.now() + SESSION_TTL_MS;
  sessions.set(token, normalized);
  return normalized.username;
}

function removeUserSessions(username) {
  sessions.forEach((entry, token) => {
    const sessionUser = getSessionMeta(entry).username;
    if (sessionUser === username) {
      sessions.delete(token);
    }
  });
}

function removeUserFromAllRooms(username) {
  rooms.forEach((room) => {
    let roomRef = room;
    if (roomRef.members.has(username)) {
      const result = removeMemberFromRoom(roomRef, username);
      if (result.deleted || !result.room) {
        return;
      }
      roomRef = result.room;
    }

    if (roomRef.joinRequests.has(username)) {
      roomRef.joinRequests.delete(username);
    }
    if (roomRef.approvedUsers instanceof Set) {
      roomRef.approvedUsers.delete(username);
    }
    if (roomRef.messageFloorByUser instanceof Map) {
      roomRef.messageFloorByUser.delete(username);
    }
    if (roomRef.pendingHostRestore === username) {
      roomRef.pendingHostRestore = null;
    }
  });
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

function roomVideoPublicToAbsolutePath(publicPath) {
  const clean = String(publicPath || "").trim();
  if (!clean || !clean.startsWith("/uploads/room-videos/")) {
    return null;
  }
  const relative = clean.replace(/^\/+/, "");
  const absolute = path.join(ROOT, relative);
  const normalizedRoot = path.resolve(ROOM_VIDEO_UPLOAD_DIR);
  const normalizedAbs = path.resolve(absolute);
  if (!normalizedAbs.startsWith(normalizedRoot + path.sep) && normalizedAbs !== normalizedRoot) {
    return null;
  }
  return absolute;
}

function removeRoomVideoAsset(room) {
  if (!room || !room.video || !room.video.src) {
    if (room) {
      room.video = null;
      room.videoSync = null;
    }
    return;
  }
  const absolutePath = roomVideoPublicToAbsolutePath(room.video.src);
  if (absolutePath && fs.existsSync(absolutePath)) {
    try {
      fs.unlinkSync(absolutePath);
    } catch (_error) {
      // Ignore cleanup errors so room operations continue.
    }
  }
  room.video = null;
  room.videoSync = null;
}

function ensureRoomVideoRuntimeState(room) {
  if (!room.video) {
    room.video = null;
  }
  if (!room.videoSync) {
    room.videoSync = null;
  }
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
  const sync = room.videoSync;
  const currentTime = sync ? getRoomVideoEffectiveTime(room) : 0;
  return {
    id: String(room.video.id || ""),
    sourceType: String(room.video.sourceType || "file"),
    youtubeId: String(room.video.youtubeId || ""),
    src: String(room.video.src || ""),
    filename: String(room.video.filename || "video"),
    mimeType: String(room.video.mimeType || "video/mp4"),
    size: Number(room.video.size || 0),
    uploadedBy: String(room.video.uploadedBy || room.host || ""),
    uploadedAt: Number(room.video.uploadedAt || room.createdAt || Date.now()),
    duration: normalizeRoomVideoDuration(room.video.duration),
    sync: sync ? {
      videoId: String(sync.videoId || ""),
      playing: Boolean(sync.playing),
      currentTime,
      playbackRate: normalizeRoomVideoPlaybackRate(sync.playbackRate),
      updatedAt: Number(sync.updatedAt || Date.now())
    } : null
  };
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
    video: formatRoomVideo(room)
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
    pendingRequests: room.host === viewer ? Array.from(room.joinRequests) : []
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
  room.messages.push({
    id,
    type: "system",
    key,
    payload,
    text: "",
    timestamp: Date.now()
  });
  room.nextMessageId += 1;
  return id;
}

function pushUserMessage(room, username, text, replyTo = null) {
  const id = room.nextMessageId;
  room.messages.push({
    id,
    type: "user",
    user: username,
    text,
    replyTo: replyTo ? { ...replyTo } : null,
    timestamp: Date.now()
  });
  room.nextMessageId += 1;
  return id;
}

function joinUserToRoom(room, username) {
  const hadHistoryFloor = room.messageFloorByUser.has(username);
  room.members.add(username);
  room.approvedUsers.add(username);
  room.joinRequests.delete(username);
  if (!room.messageFloorByUser.has(username)) {
    room.messageFloorByUser.set(username, 0);
  }
  pushSystemMessage(room, "room_joined", { user: username });
  if (room.pendingHostRestore === username && room.host !== username) {
    const previous = room.host;
    room.host = username;
    room.pendingHostRestore = null;
    pushSystemMessage(room, "host_changed", { user: username, previous });
  }
  if (hadHistoryFloor) {
    room.messageFloorByUser.set(username, room.nextMessageId - 1);
  }
}

function parseRoomPath(pathname) {
  const match = pathname.match(/^\/api\/rooms\/([A-Za-z0-9]+)(?:\/(messages|kick|requests|request-join|leave|video|video-sync|video-source))?$/);
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
  // When a player leaves, hide old chat history for their next join.
  room.messageFloorByUser.set(username, room.nextMessageId - 1);
  room.members.delete(username);
  room.joinRequests.delete(username);

  if (room.members.size === 0) {
    removeRoomVideoAsset(room);
    rooms.delete(room.code);
    return { deleted: true, room: null };
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
    const contentType = String(req.headers["content-type"] || "");
    const boundaryMatch = contentType.match(/^multipart\/form-data;\s*boundary=(?:"([^"]+)"|([^;]+))/i);
    if (!boundaryMatch) {
      reject(new Error("Invalid multipart form data"));
      return;
    }
    const boundary = String(boundaryMatch[1] || boundaryMatch[2] || "").trim();
    if (!boundary) {
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

    const chunks = [];
    let totalBytes = 0;
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
      totalBytes += chunk.length;
      if (totalBytes > maxPayloadBytes) {
        fail(new Error("Payload too large"));
        req.resume();
        return;
      }
      chunks.push(chunk);
    });

    req.on("end", () => {
      if (settled) {
        return;
      }
      try {
        const buffer = Buffer.concat(chunks);
        const delimiter = Buffer.from(`--${boundary}`);
        let cursor = buffer.indexOf(delimiter);
        if (cursor !== 0) {
          throw new Error("Invalid multipart form data");
        }

        const fields = {};
        const files = [];
        while (cursor !== -1) {
          cursor += delimiter.length;
          if (buffer[cursor] === 45 && buffer[cursor + 1] === 45) {
            break;
          }
          if (buffer[cursor] === 13 && buffer[cursor + 1] === 10) {
            cursor += 2;
          }

          const headerEnd = buffer.indexOf(Buffer.from("\r\n\r\n"), cursor);
          if (headerEnd === -1) {
            throw new Error("Invalid multipart form data");
          }

          const rawHeaders = buffer.slice(cursor, headerEnd).toString("utf8");
          const headers = {};
          rawHeaders.split("\r\n").forEach((line) => {
            const colon = line.indexOf(":");
            if (colon <= 0) {
              return;
            }
            const key = line.slice(0, colon).trim().toLowerCase();
            const value = line.slice(colon + 1).trim();
            headers[key] = value;
          });

          const disposition = String(headers["content-disposition"] || "");
          const fieldNameMatch = disposition.match(/name="([^"]+)"/i);
          if (!fieldNameMatch) {
            throw new Error("Invalid multipart form data");
          }
          const fieldName = String(fieldNameMatch[1] || "").trim();
          const fileNameMatch = disposition.match(/filename="([^"]*)"/i);

          const dataStart = headerEnd + 4;
          const nextBoundaryIndex = buffer.indexOf(Buffer.from(`\r\n--${boundary}`), dataStart);
          if (nextBoundaryIndex === -1) {
            throw new Error("Invalid multipart form data");
          }
          const data = buffer.slice(dataStart, nextBoundaryIndex);

          if (fileNameMatch && fileNameMatch[1] !== "") {
            files.push({
              fieldName,
              filename: String(fileNameMatch[1] || ""),
              contentType: String(headers["content-type"] || "application/octet-stream"),
              data
            });
          } else if (!Object.prototype.hasOwnProperty.call(fields, fieldName)) {
            fields[fieldName] = data.toString("utf8");
          }

          cursor = nextBoundaryIndex + 2;
          if (buffer.indexOf(Buffer.from(`--${boundary}--`), cursor) === cursor) {
            break;
          }
          if (buffer.indexOf(delimiter, cursor) !== cursor) {
            throw new Error("Invalid multipart form data");
          }
        }

        done({ fields, files });
      } catch (error) {
        fail(error);
      }
    });

    req.on("error", fail);
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

async function downloadYouTubeVideoToLocalFile(videoId, roomCode) {
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  const info = await ytdl.getInfo(url);
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
    const storedFileName = `room-${String(roomCode || "").toLowerCase()}-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${extension}`;
    const absolutePath = path.join(ROOM_VIDEO_UPLOAD_DIR, storedFileName);
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

  if (lastError) {
    throw lastError;
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

  const safePath = path.normalize(decodedPath).replace(/^([/\\])+/, "");
  const filePath = path.join(ROOT, safePath);
  const relativePath = path.relative(ROOT, filePath);

  if (
    !relativePath ||
    relativePath.startsWith("..") ||
    path.isAbsolute(relativePath) ||
    isBlockedStaticPath(relativePath)
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
      ext === ".html" ? "no-store" : "public, max-age=300, must-revalidate";
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
      const start = match[1] === "" ? 0 : Number(match[1]);
      const end = match[2] === "" ? stats.size - 1 : Number(match[2]);
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

  if (req.method === "POST" && pathname === "/api/register") {
    const { username, password } = await parseBody(req);
    const cleanUser = normalizeUsername(username);
    const cleanPass = String(password || "");

    if (!isValidUsername(cleanUser)) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "اسم المستخدم يجب أن يكون بين 3 و 30 حرفًا ويحتوي على أحرف/أرقام فقط.",
          "Username must be 3-30 characters and use only letters, numbers, _ or -."
        )
      });
      return;
    }
    if (!isValidPassword(cleanPass)) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "كلمة المرور يجب أن تكون بين 4 و 128 حرفًا.",
          "Password must be 4-128 characters."
        )
      });
      return;
    }
    if (users.has(cleanUser)) {
      sendJson(res, 409, {
        error: i18n(req, "اسم المستخدم مستخدم بالفعل.", "Username is already in use.")
      });
      return;
    }

    const passwordRecord = createPasswordRecord(cleanPass);
    users.set(cleanUser, {
      passwordHash: passwordRecord.passwordHash,
      passwordSalt: passwordRecord.passwordSalt,
      createdAt: Date.now(),
      displayName: cleanUser,
      avatarDataUrl: ""
    });
    saveUsers();
    const token = createSession(cleanUser);
    sendJson(res, 201, { token, username: cleanUser, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/login") {
    const { username, password } = await parseBody(req);
    const cleanUser = normalizeUsername(username);
    const cleanPass = String(password || "");

    if (!cleanUser || !cleanPass || cleanPass.length > PASSWORD_MAX_LENGTH) {
      sendJson(res, 401, {
        error: i18n(req, "بيانات الدخول غير صحيحة.", "Invalid login credentials.")
      });
      return;
    }

    if (!users.has(cleanUser)) {
      sendJson(res, 401, {
        error: i18n(req, "بيانات الدخول غير صحيحة.", "Invalid login credentials.")
      });
      return;
    }
    const user = users.get(cleanUser);
    if (!verifyPassword(cleanUser, user, cleanPass)) {
      sendJson(res, 401, {
        error: i18n(req, "بيانات الدخول غير صحيحة.", "Invalid login credentials.")
      });
      return;
    }
    if (!isSupervisor(cleanUser) && isBanned(cleanUser)) {
      sendBannedResponse(req, res, cleanUser);
      return;
    }

    const token = createSession(cleanUser);
    sendJson(res, 200, { token, username: cleanUser, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/guest-login") {
    const { name } = await parseBody(req);
    const cleanName = sanitizeGuestDisplayName(name);
    if (cleanName.length < 2 || cleanName.length > 30) {
      sendJson(res, 400, {
        error: i18n(req, "الاسم يجب أن يكون بين 2 و 30 حرفًا.", "Name must be 2-30 characters.")
      });
      return;
    }
    const username = createGuestUsername(cleanName);
    guestUsers.set(username, {
      createdAt: Date.now(),
      displayName: cleanName,
      avatarDataUrl: ""
    });
    const token = createSession(username, { isGuest: true });
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
      sessions.delete(token);
      if (meta.isGuest && meta.username && !hasActiveSessionForUser(meta.username, token)) {
        cleanupGuestUser(meta.username);
      }
    }
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "POST" && pathname === "/api/ban-appeals") {
    const { username: rawUsername, reason } = await parseBody(req);
    const username = normalizeUsername(rawUsername);
    const cleanReason = String(reason || "").trim();
    if (!isValidUsername(username) || !users.has(username)) {
      sendJson(res, 404, {
        error: i18n(req, "الحساب غير موجود.", "Account not found.")
      });
      return;
    }
    if (!isBanned(username)) {
      sendJson(res, 400, {
        error: i18n(req, "هذا الحساب ليس محظورًا.", "This account is not banned.")
      });
      return;
    }
    if (cleanReason.length < 8 || cleanReason.length > 500) {
      sendJson(res, 400, {
        error: i18n(req, "سبب الطلب يجب أن يكون بين 8 و 500 حرف.", "Appeal reason must be 8-500 characters.")
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
    sendJson(res, 202, { ok: true, request: formatUnbanRequest(unbanRequests.get(username)) });
    return;
  }

  const appealPrefix = "/api/ban-appeals/";
  if (req.method === "GET" && pathname.startsWith(appealPrefix)) {
    const decodedUsername = safeDecodeURIComponent(pathname.slice(appealPrefix.length));
    if (decodedUsername === null) {
      sendJson(res, 400, {
        error: i18n(req, "معرف المستخدم غير صالح.", "Invalid user identifier.")
      });
      return;
    }
    const username = normalizeUsername(decodedUsername);
    if (!isValidUsername(username) || !users.has(username)) {
      sendJson(res, 404, {
        error: i18n(req, "الحساب غير موجود.", "Account not found.")
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
        error: i18n(req, "غير مصرح.", "Unauthorized.")
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
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
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
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
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

  if (req.method === "POST" && pathname === "/api/admin/site-announcement") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { text } = await parseBody(req);
    const cleanText = String(text || "").trim();
    if (!cleanText) {
      sendJson(res, 400, {
        error: i18n(req, "الرسالة العامة مطلوبة.", "Announcement text is required.")
      });
      return;
    }
    if (cleanText.length > 500) {
      sendJson(res, 400, {
        error: i18n(req, "الرسالة العامة طويلة جدًا (الحد 500 حرف).", "Announcement is too long (max 500 chars).")
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
    sendJson(res, 201, { ok: true, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/ban-user") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, reason } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanReason = String(reason || "").trim();
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "الحساب غير موجود.", "Account not found.") });
      return;
    }
    if (isSupervisor(target)) {
      sendJson(res, 400, { error: i18n(req, "لا يمكن حظر حساب المشرف.", "Supervisor account cannot be banned.") });
      return;
    }
    if (cleanReason.length < 3 || cleanReason.length > 300) {
      sendJson(res, 400, { error: i18n(req, "سبب الحظر يجب أن يكون بين 3 و 300 حرف.", "Ban reason must be 3-300 characters.") });
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
    sendJson(res, 200, { ok: true, ban: formatBanRecord(getBanRecord(target)) });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/unban-user") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, note } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanNote = String(note || "").trim();
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "الحساب غير موجود.", "Account not found.") });
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
    sendJson(res, 200, { ok: true, username: target });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/delete-user") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "الحساب غير موجود.", "Account not found.") });
      return;
    }
    if (isSupervisor(target)) {
      sendJson(res, 400, { error: i18n(req, "لا يمكن حذف حساب المشرف.", "Supervisor account cannot be deleted.") });
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
    sendJson(res, 200, { ok: true, username: target });
    return;
  }

  if (req.method === "POST" && pathname === "/api/admin/ban-appeals/decision") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
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
      sendJson(res, 404, { error: i18n(req, "طلب غير موجود.", "Request not found.") });
      return;
    }
    if (cleanAction !== "approve" && cleanAction !== "reject") {
      sendJson(res, 400, { error: i18n(req, "إجراء غير صالح.", "Invalid action.") });
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
    sendJson(res, 200, { ok: true, request: formatUnbanRequest(request) });
    return;
  }

  if (req.method === "GET" && pathname === "/api/profile") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
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
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
    const isRegistered = users.has(username);
    const isGuest = guestUsers.has(username);
    if (!isRegistered && !isGuest) {
      sendJson(res, 404, { error: i18n(req, "الحساب غير موجود.", "Account not found.") });
      return;
    }
    const { displayName, avatarDataUrl } = await parseBody(req);
    const user = isRegistered ? users.get(username) : guestUsers.get(username);

    if (displayName !== undefined) {
      const cleanName = String(displayName || "").trim();
      if (cleanName.length < 2 || cleanName.length > 30) {
        sendJson(res, 400, {
          error: i18n(req, "الاسم الظاهر يجب أن يكون بين 2 و 30 حرفًا.", "Display name must be 2 to 30 characters.")
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
            error: i18n(req, "الصورة غير صالحة أو كبيرة جدًا.", "Avatar is invalid or too large.")
          });
          return;
        }
        user.avatarDataUrl = rawAvatar;
      }
    }

    if (isRegistered) {
      users.set(username, user);
      saveUsers();
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
      sendJson(res, 401, { error: i18n(req, "غير مصرح.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, viewer)) {
      return;
    }
    const rawTarget = pathname.slice(userProfilePrefix.length, pathname.length - userProfileSuffix.length);
    const decodedTarget = safeDecodeURIComponent(String(rawTarget || ""));
    if (decodedTarget === null) {
      sendJson(res, 400, { error: i18n(req, "معرف المستخدم غير صالح.", "Invalid user identifier.") });
      return;
    }
    const target = normalizeUsername(decodedTarget);
    if (!isValidUsername(target)) {
      sendJson(res, 400, { error: i18n(req, "معرف المستخدم غير صالح.", "Invalid user identifier.") });
      return;
    }
    if (!users.has(target) && !guestUsers.has(target)) {
      sendJson(res, 404, { error: i18n(req, "المستخدم غير موجود.", "User not found.") });
      return;
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
        error: i18n(req, "غير مصرح.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }

    const { roomName } = await parseBody(req);
    const cleanRoomName = String(roomName || "").trim();
    if (cleanRoomName.length > ROOM_NAME_MAX_LENGTH) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "اسم الغرفة طويل جدًا (الحد 40 حرفًا).",
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
      name: cleanRoomName || i18n(req, "غرفة جديدة", "New Room"),
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
      videoSync: null
    };
    pushSystemMessage(room, "room_created", { user: username });
    rooms.set(code, room);
    sendJson(res, 201, { room: formatRoom(room, username) });
    return;
  }

  if (req.method === "GET" && pathname === "/api/rooms") {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "غير مصرح.", "Unauthorized.")
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
        error: i18n(req, "غير مصرح.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }

    const { code } = await parseBody(req);
    const roomCode = String(code || "").trim().toUpperCase();
    if (!roomCode || !rooms.has(roomCode)) {
      sendJson(res, 404, {
        error: i18n(req, "الغرفة غير موجودة.", "Room not found.")
      });
      return;
    }

    const room = rooms.get(roomCode);
    ensureRoomRuntimeState(room);

    if (!room.members.has(username)) {
      if (room.approvedUsers.has(username) || isSupervisor(username)) {
        joinUserToRoom(room, username);
      } else if (room.joinRequests.has(username)) {
        sendJson(res, 403, {
          error: i18n(req, "طلب الانضمام لا يزال قيد الانتظار.", "Your join request is still pending.")
        });
        return;
      } else {
        sendJson(res, 403, {
          error: i18n(req, "لا يمكنك الانضمام حتى يوافق القائد على طلبك.", "You cannot join until the leader approves your request.")
        });
        return;
      }
    }
    sendJson(res, 200, { room: formatRoom(room, username) });
    return;
  }

  const roomPath = parseRoomPath(pathname);
  if (roomPath) {
    const username = getUserFromRequest(req);
    if (!username) {
      sendJson(res, 401, {
        error: i18n(req, "غير مصرح.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }

    if (!rooms.has(roomPath.code)) {
      sendJson(res, 404, {
        error: i18n(req, "الغرفة غير موجودة.", "Room not found.")
      });
      return;
    }
    const room = rooms.get(roomPath.code);
    ensureRoomRuntimeState(room);
    if (req.method === "POST" && roomPath.action === "request-join") {
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
          sendJson(res, 202, {
            ok: true,
            status: "pending"
          });
          return;
        }
        joinUserToRoom(room, username);
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
      sendJson(res, 202, {
        ok: true,
        status: "pending"
      });
      return;
    }

    if (!room.members.has(username)) {
      sendJson(res, 403, {
        error: i18n(req, "انضم إلى هذه الغرفة أولًا.", "Join this room first.")
      });
      return;
    }

    if (req.method === "POST" && roomPath.action === "leave") {
      const result = removeMemberFromRoom(room, username);
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
      const floor = room.messageFloorByUser.get(username) || 0;
      const fromId = Math.max(since, floor);
      const messages = room.messages.filter((msg) => msg.id > fromId);
      sendJson(res, 200, { room: formatRoom(room, username), messages, announcement: getLiveSiteAnnouncement() });
      return;
    }

    if (req.method === "GET" && roomPath.action === "requests") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "فقط قائد الغرفة يمكنه عرض الطلبات.", "Only the room leader can view requests.")
        });
        return;
      }
      sendJson(res, 200, { requests: Array.from(room.joinRequests) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "requests") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "فقط قائد الغرفة يمكنه إدارة الطلبات.", "Only the room leader can manage requests.")
        });
        return;
      }
      const { username: targetRaw, action } = await parseBody(req);
      const target = normalizeUsername(targetRaw);
      const cleanAction = String(action || "").trim().toLowerCase();
      if (!isValidUsername(target)) {
        sendJson(res, 400, {
          error: i18n(req, "اسم اللاعب المستهدف مطلوب.", "Target username is required.")
        });
        return;
      }
      if (!room.joinRequests.has(target)) {
        sendJson(res, 404, {
          error: i18n(req, "لا يوجد طلب انضمام لهذا اللاعب.", "No join request found for this player.")
        });
        return;
      }
      if (cleanAction !== "approve" && cleanAction !== "reject") {
        sendJson(res, 400, {
          error: i18n(req, "إجراء غير صالح.", "Invalid action.")
        });
        return;
      }

      room.joinRequests.delete(target);
      if (cleanAction === "approve") {
        room.approvedUsers.add(target);
        if (!room.messageFloorByUser.has(target)) {
          room.messageFloorByUser.set(target, 0);
        }
        const wasMember = room.members.has(target);
        room.members.add(target);
        if (!wasMember) {
          pushSystemMessage(room, "room_joined", { user: target });
        }
        sendJson(res, 200, { ok: true, status: "approved", room: formatRoom(room, username) });
        return;
      }
      if (cleanAction === "reject") {
        sendJson(res, 200, { ok: true, status: "rejected", room: formatRoom(room, username) });
        return;
      }
    }

    if (req.method === "POST" && roomPath.action === "messages") {
      const { text, clientMessageId, replyToMessageId } = await parseBody(req);
      const messageText = String(text || "").trim();
      if (!messageText) {
        sendJson(res, 400, {
          error: i18n(req, "الرسالة فارغة.", "Message is empty.")
        });
        return;
      }
      if (messageText.length > 300) {
        sendJson(res, 400, {
          error: i18n(req, "الرسالة طويلة جدًا.", "Message is too long.")
        });
        return;
      }

      const normalizedReplyToMessageId = normalizeReplyToMessageId(replyToMessageId);
      if (Number.isNaN(normalizedReplyToMessageId)) {
        sendJson(res, 400, {
          error: i18n(req, "معرف الرد غير صالح.", "Invalid reply identifier.")
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
              "لا يمكن الرد على هذه الرسالة.",
              "You can only reply to an existing player message."
            )
          });
          return;
        }
      }

      const normalizedClientMessageId = normalizeClientMessageId(clientMessageId);
      if (clientMessageId !== undefined && !normalizedClientMessageId) {
        sendJson(res, 400, {
          error: i18n(req, "معرف الرسالة غير صالح.", "Invalid message identifier.")
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

      const messageId = pushUserMessage(room, username, messageText, replyTo);
      if (dedupeKey) {
        room.clientMessageLog.set(dedupeKey, {
          messageId,
          timestamp: Date.now()
        });
      }
      sendJson(res, 201, { ok: true, messageId });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "فقط قائد الغرفة يمكنه رفع الفيديو.", "Only the room leader can upload videos.")
        });
        return;
      }

      const multipart = await parseMultipartFormData(req, ROOM_VIDEO_MAX_BYTES);
      const uploaded = multipart.files.find((file) => file.fieldName === "video") || multipart.files[0] || null;
      if (!uploaded || !uploaded.data || uploaded.data.length === 0) {
        sendJson(res, 400, {
          code: "VIDEO_FILE_REQUIRED",
          error: i18n(req, "اختر ملف فيديو صالح.", "Please choose a valid video file.")
        });
        return;
      }
      if (uploaded.data.length > ROOM_VIDEO_MAX_BYTES) {
        sendJson(res, 413, {
          code: "VIDEO_TOO_LARGE",
          error: i18n(req, "حجم الفيديو كبير جدًا (الحد 80MB).", "Video file is too large (max 80MB).")
        });
        return;
      }

      const mimeType = normalizeVideoMimeType(uploaded.contentType);
      if (!ROOM_VIDEO_ALLOWED_MIME_TYPES.has(mimeType)) {
        sendJson(res, 415, {
          code: "VIDEO_INVALID_TYPE",
          error: i18n(req, "نوع الفيديو غير مدعوم. استخدم MP4 أو WebM أو OGG.", "Unsupported video type. Use MP4, WebM or OGG.")
        });
        return;
      }

      ensureUploadsDir();
      const cleanFileName = sanitizeUploadedFilename(uploaded.filename || "room-video");
      const extension = normalizeVideoExtension(cleanFileName, mimeType);
      const storedFileName = `room-${room.code.toLowerCase()}-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${extension}`;
      const absolutePath = path.join(ROOM_VIDEO_UPLOAD_DIR, storedFileName);
      fs.writeFileSync(absolutePath, uploaded.data);

      const normalizedDuration = normalizeRoomVideoDuration(multipart.fields.duration);
      removeRoomVideoAsset(room);
      room.video = {
        id: randomToken(),
        sourceType: "file",
        youtubeId: "",
        src: `/uploads/room-videos/${storedFileName}`,
        filename: cleanFileName,
        mimeType,
        size: uploaded.data.length,
        uploadedBy: username,
        uploadedAt: Date.now(),
        duration: normalizedDuration
      };
      room.videoSync = {
        videoId: room.video.id,
        playing: false,
        baseTime: 0,
        playbackRate: 1,
        updatedAt: Date.now()
      };

      sendJson(res, 201, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-source") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "فقط قائد الغرفة يمكنه ضبط مصدر الفيديو.", "Only the room leader can set video source.")
        });
        return;
      }

      const { input } = await parseBody(req);
      const rawInput = String(input || "").trim();
      if (rawInput.length < 2) {
        sendJson(res, 400, {
          code: "YOUTUBE_INPUT_REQUIRED",
          error: i18n(req, "أدخل رابط يوتيوب أو نص بحث.", "Enter a YouTube URL or search text.")
        });
        return;
      }

      let resolved = null;
      try {
        resolved = await resolveYouTubeInput(rawInput);
      } catch (_error) {
        sendJson(res, 502, {
          code: "YOUTUBE_RESOLVE_FAILED",
          error: i18n(req, "تعذر الوصول إلى يوتيوب حاليًا. حاول مرة أخرى.", "Could not reach YouTube right now. Try again.")
        });
        return;
      }

      if (!resolved?.videoId) {
        sendJson(res, 404, {
          code: "YOUTUBE_NOT_FOUND",
          error: i18n(req, "لم يتم العثور على فيديو مناسب.", "No matching YouTube video was found.")
        });
        return;
      }

      ensureUploadsDir();
      let downloaded = null;
      try {
        downloaded = await downloadYouTubeVideoToLocalFile(resolved.videoId, room.code);
      } catch (error) {
        const code = String(error?.code || "");
        if (code === "YOUTUBE_VIDEO_TOO_LARGE") {
          sendJson(res, 413, {
            code: "YOUTUBE_VIDEO_TOO_LARGE",
            error: i18n(
              req,
              "فيديو يوتيوب أكبر من الحد المسموح (80MB). اختر فيديو أقصر أو بجودة أقل.",
              "YouTube video exceeds max allowed size (80MB). Choose a shorter or lower-quality video."
            )
          });
          return;
        }
        if (code === "YOUTUBE_FORMAT_UNSUPPORTED") {
          sendJson(res, 415, {
            code: "YOUTUBE_FORMAT_UNSUPPORTED",
            error: i18n(req, "تعذر العثور على صيغة فيديو مدعومة لهذا الرابط.", "No supported downloadable format was found for this video.")
          });
          return;
        }
        sendJson(res, 502, {
          code: "YOUTUBE_DOWNLOAD_FAILED",
          error: i18n(req, "تعذر تنزيل فيديو يوتيوب حاليًا. حاول فيديو آخر.", "Could not download this YouTube video right now. Try another one.")
        });
        return;
      }

      removeRoomVideoAsset(room);
      room.video = {
        id: randomToken(),
        sourceType: "file",
        youtubeId: "",
        src: `/uploads/room-videos/${downloaded.storedFileName}`,
        filename: downloaded.cleanFileName,
        mimeType: downloaded.mimeType,
        size: downloaded.downloadedBytes,
        uploadedBy: username,
        uploadedAt: Date.now(),
        duration: downloaded.durationSec
      };
      room.videoSync = {
        videoId: room.video.id,
        playing: false,
        baseTime: 0,
        playbackRate: 1,
        updatedAt: Date.now()
      };

      sendJson(res, 201, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-sync") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "فقط قائد الغرفة يمكنه مزامنة الفيديو.", "Only the room leader can sync the video.")
        });
        return;
      }
      if (!room.video) {
        sendJson(res, 404, {
          code: "VIDEO_NOT_FOUND",
          error: i18n(req, "لا يوجد فيديو في الغرفة.", "No room video is available.")
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
          updatedAt: Date.now()
        };
      }

      const { action, currentTime, playbackRate, duration, videoId } = await parseBody(req);
      const cleanAction = String(action || "").trim().toLowerCase();
      if (!["play", "pause", "seek", "rate", "stop"].includes(cleanAction)) {
        sendJson(res, 400, {
          error: i18n(req, "إجراء مزامنة فيديو غير صالح.", "Invalid video sync action.")
        });
        return;
      }

      const clientVideoId = String(videoId || "").trim();
      if (clientVideoId && clientVideoId !== String(room.video.id || "")) {
        sendJson(res, 409, {
          code: "VIDEO_STALE",
          error: i18n(req, "هذه النسخة من الفيديو قديمة. قم بتحديث الغرفة.", "This video version is outdated. Refresh room data.")
        });
        return;
      }

      const now = Date.now();
      const effective = getRoomVideoEffectiveTime(room, now);
      const nextRate = normalizeRoomVideoPlaybackRate(
        playbackRate !== undefined ? playbackRate : room.videoSync.playbackRate
      );
      const nextTime = currentTime !== undefined ? clampRoomVideoTime(room, currentTime) : effective;
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
        if (playbackRate !== undefined) {
          room.videoSync.playbackRate = nextRate;
        }
      } else if (cleanAction === "rate") {
        room.videoSync.baseTime = effective;
        room.videoSync.playbackRate = nextRate;
        room.videoSync.updatedAt = now;
      } else if (cleanAction === "stop") {
        room.videoSync.playing = false;
        room.videoSync.baseTime = 0;
        room.videoSync.playbackRate = 1;
        room.videoSync.updatedAt = now;
      }

      sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "kick") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "فقط قائد الغرفة يمكنه طرد اللاعبين.", "Only the room leader can kick players.")
        });
        return;
      }

      const { username: targetRaw } = await parseBody(req);
      const target = normalizeUsername(targetRaw);
      if (!isValidUsername(target)) {
        sendJson(res, 400, {
          error: i18n(req, "اسم اللاعب المستهدف مطلوب.", "Target username is required.")
        });
        return;
      }
      if (target === room.host) {
        sendJson(res, 400, {
          error: i18n(req, "لا يمكن للقائد طرد نفسه.", "Leader cannot kick themselves.")
        });
        return;
      }
      if (isSupervisor(target)) {
        sendJson(res, 403, {
          code: "SUPERVISOR_KICK_FORBIDDEN",
          error: i18n(req, "لا يمكن طرد مشرف من الغرفة.", "A supervisor cannot be kicked from this room.")
        });
        return;
      }
      if (!room.members.has(target)) {
        sendJson(res, 404, {
          error: i18n(req, "اللاعب غير موجود في هذه الغرفة.", "Player is not in this room.")
        });
        return;
      }

      room.members.delete(target);
      room.approvedUsers.delete(target);
      room.messageFloorByUser.set(target, room.nextMessageId - 1);
      if (room.pendingHostRestore === target) {
        room.pendingHostRestore = null;
      }
      room.joinRequests.delete(target);
      pushSystemMessage(room, "player_kicked", { user: target });
      sendJson(res, 200, { room: formatRoom(room, username) });
      return;
    }

    sendJson(res, 404, {
      error: i18n(req, "مسار الغرفة غير موجود.", "Room endpoint not found.")
    });
    return;
  }

  sendJson(res, 404, {
    error: i18n(req, "المسار غير موجود.", "Endpoint not found.")
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const parsedUrl = new URL(req.url, "http://localhost");
    const pathname = parsedUrl.pathname;
    if (pathname.startsWith("/api/")) {
      await handleApi(req, res);
      return;
    }
    serveStatic(pathname, req, res);
  } catch (error) {
    if (error.message === "Payload too large") {
      sendJson(res, 413, {
        error: i18n(req, "حجم البيانات كبير جدًا.", "Payload is too large.")
      });
      return;
    }
    if (error.message === "Invalid JSON") {
      sendJson(res, 400, {
        error: i18n(req, "صيغة JSON غير صحيحة.", "Invalid JSON body.")
      });
      return;
    }
    if (error.message === "Invalid multipart form data") {
      sendJson(res, 400, {
        error: i18n(req, "ملف الفيديو غير صالح.", "Invalid video upload payload.")
      });
      return;
    }
    sendJson(res, 500, {
      error: i18n(req, "خطأ داخلي في الخادم.", "Internal server error.")
    });
  }
});

loadUsers();
loadModeration();
ensureUploadsDir();

server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

