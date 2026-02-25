const http = require("http");
const fs = require("fs");
const https = require("https");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
if (!process.env.YTDL_NO_DEBUG_FILE) {
  process.env.YTDL_NO_DEBUG_FILE = "1";
}
if (!process.env.YTDL_NO_UPDATE) {
  process.env.YTDL_NO_UPDATE = "1";
}
const ytdl = require("@distube/ytdl-core");
const Busboy = require("busboy");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_DIR = path.resolve(String(process.env.DATA_DIR || path.join(ROOT, "data")));
const UPLOADS_DIR = path.resolve(String(process.env.UPLOADS_DIR || path.join(ROOT, "uploads")));
const ROOM_VIDEO_PUBLIC_PREFIX = "/uploads/room-videos/";
const ROOM_VIDEO_UPLOAD_DIR_DEFAULT = path.join(UPLOADS_DIR, "room-videos");
const ROOM_VIDEO_UPLOAD_DIR_FALLBACK = path.join(os.tmpdir(), "bednanl3b-room-videos");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const USERS_BACKUP_FILE = path.join(DATA_DIR, "users.backup.json");
const MODERATION_FILE = path.join(DATA_DIR, "moderation.json");
const YOUTUBE_CACHE_FILE = path.join(DATA_DIR, "youtube-cache.json");
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
const YOUTUBE_SEARCH_TIMEOUT_MS = 9000;
const YOUTUBE_DOWNLOAD_TIMEOUT_MS = 1000 * 60 * 4;
const YOUTUBE_INPUT_MAX_LENGTH = 300;
const YOUTUBE_VIDEO_ID_REGEX = /^[A-Za-z0-9_-]{11}$/;
const YOUTUBE_CACHE_MAX_ITEMS = 180;
const YOUTUBE_CACHE_MAX_AGE_MS = 1000 * 60 * 60 * 24 * 30;
const YOUTUBE_PROXY_TTL_MS = 1000 * 60 * 60 * 6;
const YT_DLP_BINARY = String(process.env.YT_DLP_BINARY || "yt-dlp").trim() || "yt-dlp";
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
const roomEmptyDeletionTimers = new Map();
const userLastSeen = new Map();
const bannedUsers = new Map();
const unbanRequests = new Map();
const youtubeVideoCache = new Map();
const youtubeProxyStreams = new Map();
let siteAnnouncement = null;
let activeRoomVideoUploadDir = ROOM_VIDEO_UPLOAD_DIR_DEFAULT;
let ytDlpCommandOverride = null;

function parseJsonWithOptionalBom(raw) {
  return JSON.parse(String(raw || "").replace(/^\uFEFF/, ""));
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
      avatarDataUrl: typeof item.avatarDataUrl === "string" ? item.avatarDataUrl : ""
    });
  });
}

function loadUsers() {
  ensureDataDir();
  try {
    if (!fs.existsSync(USERS_FILE)) {
      writeTextFileAtomic(USERS_FILE, "[]");
      writeTextFileAtomic(USERS_BACKUP_FILE, "[]");
      return;
    }
    const raw = fs.readFileSync(USERS_FILE, "utf8");
    const list = parseJsonWithOptionalBom(raw);
    if (!Array.isArray(list)) {
      throw new Error("users.json must contain an array.");
    }
    hydrateUsersFromList(list);
    writeTextFileAtomic(USERS_BACKUP_FILE, JSON.stringify(list, null, 2));
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
    } catch (backupError) {
      console.error("Failed to recover users from backup:", backupError.message);
    }
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
    const serialized = JSON.stringify(list, null, 2);
    writeTextFileAtomic(USERS_FILE, serialized);
    writeTextFileAtomic(USERS_BACKUP_FILE, serialized);
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
    sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½. أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Video stream link expired. Please set the video again.") });
    return;
  }

  const method = req.method === "HEAD" ? "HEAD" : "GET";
  const rangeHeader = String(req.headers.range || "").trim();

  let info;
  try {
    info = await getYouTubeInfoRobust(item.videoId);
  } catch (error) {
    sendJson(res, 502, {
      error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Could not proxy YouTube video right now.")
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
      error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Could not find a compatible playable format.")
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

function normalizeDisplayName(value, fallbackUsername = "") {
  const fallback = String(fallbackUsername || "").trim().toLowerCase().slice(0, 30);
  const clean = String(value || "").trim().slice(0, 30);
  if (!clean || looksLikeMojibake(clean)) {
    return fallback || SUPERVISOR_USERNAME;
  }
  return clean;
}

const I18N_AR_BY_EN = Object.freeze({
  "A supervisor cannot be kicked from this room.": "لا يمكن طرد المشرف من هذه الغرفة.",
  "Account not found.": "الحساب غير موجود.",
  "Announcement is too long (max 500 chars).": "الرسالة العامة طويلة جدًا (الحد الأقصى 500 حرف).",
  "Announcement text is required.": "نص الرسالة العامة مطلوب.",
  "Appeal reason must be 8-500 characters.": "سبب طلب رفع الحظر يجب أن يكون بين 8 و500 حرف.",
  "Avatar is invalid or too large.": "الصورة الشخصية غير صالحة أو حجمها كبير جدًا.",
  "Ban reason must be 3-300 characters.": "سبب الحظر يجب أن يكون بين 3 و300 حرف.",
  "Could not download this YouTube video right now. Try another one.": "تعذر تنزيل فيديو يوتيوب الآن. جرّب فيديو آخر.",
  "Could not find a compatible playable format.": "تعذر العثور على صيغة تشغيل متوافقة.",
  "Could not proxy YouTube video right now.": "تعذر تشغيل فيديو يوتيوب عبر الخادم الآن.",
  "Could not reach YouTube right now. Try again.": "تعذر الاتصال بيوتيوب الآن. حاول مرة أخرى.",
  "Display name must be 2 to 30 characters.": "الاسم الظاهر يجب أن يكون من 2 إلى 30 حرفًا.",
  "Endpoint not found.": "نقطة الوصول غير موجودة.",
  "Enter a YouTube URL or search text.": "أدخل رابط يوتيوب أو نص بحث.",
  "Internal server error.": "حدث خطأ داخلي في الخادم.",
  "Invalid JSON body.": "بيانات JSON غير صالحة.",
  "Invalid action.": "الإجراء غير صالح.",
  "Invalid login credentials.": "بيانات تسجيل الدخول غير صحيحة.",
  "Invalid message identifier.": "معرّف الرسالة غير صالح.",
  "Invalid reaction payload.": "بيانات التفاعل غير صالحة.",
  "Invalid reply identifier.": "معرّف الرد غير صالح.",
  "Invalid user identifier.": "معرّف المستخدم غير صالح.",
  "Invalid video stream link.": "رابط بث الفيديو غير صالح.",
  "Invalid video sync action.": "إجراء مزامنة الفيديو غير صالح.",
  "Invalid video upload payload.": "بيانات رفع الفيديو غير صالحة.",
  "Join this room first.": "انضم إلى هذه الغرفة أولًا.",
  "Leader cannot kick themselves.": "لا يمكن للقائد طرد نفسه.",
  "Message is empty.": "الرسالة فارغة.",
  "Message is too long.": "الرسالة طويلة جدًا.",
  "Name must be 2-30 characters.": "الاسم يجب أن يكون بين 2 و30 حرفًا.",
  "New Room": "غرفة جديدة",
  "No eligible message was found for reaction.": "لم يتم العثور على رسالة مناسبة للتفاعل.",
  "No join request found for this player.": "لا يوجد طلب انضمام لهذا اللاعب.",
  "No matching YouTube video was found.": "لم يتم العثور على فيديو يوتيوب مطابق.",
  "No room video is available.": "لا يوجد فيديو متاح في الغرفة.",
  "No supported downloadable video format was found.": "لم يتم العثور على صيغة فيديو قابلة للتنزيل ومدعومة.",
  "Only the room leader can kick players.": "فقط قائد الغرفة يمكنه طرد اللاعبين.",
  "Only the room leader can manage requests.": "فقط قائد الغرفة يمكنه إدارة الطلبات.",
  "Only the room leader can set video source.": "فقط قائد الغرفة يمكنه تحديد مصدر الفيديو.",
  "Only the room leader can sync the video.": "فقط قائد الغرفة يمكنه مزامنة الفيديو.",
  "Only the room leader can upload videos.": "فقط قائد الغرفة يمكنه رفع الفيديو.",
  "Only the room leader can remove videos.": "فقط قائد الغرفة يمكنه حذف الفيديو.",
  "Only the room leader can view requests.": "فقط قائد الغرفة يمكنه عرض الطلبات.",
  "Only the room leader can transfer leadership.": "فقط قائد الغرفة يمكنه نقل القيادة.",
  "Only a supervisor in this room can claim leadership.": "فقط مشرف موجود داخل الغرفة يمكنه امتلاك القيادة.",
  "Payload is too large.": "حجم البيانات المرسلة كبير جدًا.",
  "Player is not in this room.": "اللاعب ليس في هذه الغرفة.",
  "Please choose a valid video file.": "يرجى اختيار ملف فيديو صالح.",
  "Request not found.": "الطلب غير موجود.",
  "Room endpoint not found.": "نقطة وصول الغرفة غير موجودة.",
  "Room not found.": "الغرفة غير موجودة.",
  "Server storage is unavailable right now.": "مساحة تخزين الخادم غير متاحة حاليًا.",
  "Supervisor account cannot be banned.": "لا يمكن حظر حساب المشرف.",
  "Supervisor account cannot be deleted.": "لا يمكن حذف حساب المشرف.",
  "Target username is required.": "اسم المستخدم المستهدف مطلوب.",
  "Leader transfer target must be another member.": "يجب اختيار عضو آخر داخل الغرفة لنقل القيادة.",
  "This account is not banned.": "هذا الحساب غير محظور.",
  "This action is supervisor-only.": "هذا الإجراء مخصص للمشرف فقط.",
  "This video version is outdated. Refresh room data.": "إصدار الفيديو هذا قديم. حدّث بيانات الغرفة.",
  "Unauthorized.": "غير مصرح.",
  "Unsupported video type. Use MP4, WebM or OGG.": "نوع الفيديو غير مدعوم. استخدم MP4 أو WebM أو OGG.",
  "User not found.": "المستخدم غير موجود.",
  "Username must be 3-30 characters and use only English letters and numbers.": "اسم المستخدم يجب أن يكون من 3 إلى 30 حرفًا، ويحتوي فقط على أحرف إنجليزية وأرقام.",
  "Username is already in use.": "اسم المستخدم مستخدم بالفعل.",
  "Video file is too large (max 1GB).": "ملف الفيديو كبير جدًا (الحد الأقصى 1GB).",
  "Video stream link expired. Please set the video again.": "انتهت صلاحية رابط بث الفيديو. يرجى تعيين الفيديو مرة أخرى.",
  "You cannot join until the leader approves your request.": "لا يمكنك الانضمام حتى يوافق القائد على طلبك.",
  "YouTube video is too large (max 1GB).": "فيديو يوتيوب كبير جدًا (الحد الأقصى 1GB).",
  "Your join request is still pending.": "طلب انضمامك ما يزال قيد الانتظار."
});

function i18n(req, ar, en) {
  if (getLang(req) === "en") {
    return en;
  }
  if (Object.prototype.hasOwnProperty.call(I18N_AR_BY_EN, en)) {
    return I18N_AR_BY_EN[en];
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
      "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½. أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.",
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
    error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "This action is supervisor-only.")
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

function removeRoomVideoAsset(room) {
  if (!room || !room.video || !room.video.src) {
    if (room) {
      room.video = null;
      room.videoSync = null;
    }
    return;
  }
  const shouldPreserveCachedFile = Boolean(room.video.isYouTubeCached || isVideoSrcInYouTubeCache(room.video.src));
  const absolutePath = shouldPreserveCachedFile ? null : roomVideoPublicToAbsolutePath(room.video.src);
  if (absolutePath && fs.existsSync(absolutePath)) {
    try {
      fs.unlinkSync(absolutePath);
    } catch (_error) {
      // Ignore cleanup errors so room operations continue.
    }
  }
  const proxyId = extractYouTubeProxyIdFromSrc(room.video.src);
  if (proxyId) {
    youtubeProxyStreams.delete(proxyId);
  }
  room.video = null;
  room.videoSync = null;
}

function setRoomVideoFromYouTubeProxy(room, videoId, direct, uploadedBy) {
  const proxy = registerYouTubeProxyStream(videoId, direct);
  removeRoomVideoAsset(room);
  room.video = {
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
  };
  room.videoSync = {
    videoId: room.video.id,
    playing: false,
    baseTime: 0,
    playbackRate: 1,
    updatedAt: Date.now(),
    clientNow: 0,
    clientSeq: 0
  };
}

function setRoomVideoFromYouTubeEmbed(room, videoId, uploadedBy, label = "", duration = 0) {
  const cleanVideoId = String(videoId || "").trim();
  removeRoomVideoAsset(room);
  room.video = {
    id: randomToken(),
    sourceType: "youtube",
    youtubeId: cleanVideoId,
    src: `https://www.youtube.com/watch?v=${cleanVideoId}`,
    filename: sanitizeUploadedFilename(label || `youtube-${cleanVideoId}`),
    mimeType: "video/youtube",
    size: 0,
    uploadedBy,
    uploadedAt: Date.now(),
    duration: normalizeRoomVideoDuration(duration),
    isYouTubeCached: false,
    isYouTubeProxy: false
  };
  room.videoSync = {
    videoId: room.video.id,
    playing: false,
    baseTime: 0,
    playbackRate: 1,
    updatedAt: Date.now(),
    clientNow: 0,
    clientSeq: 0
  };
}

function ensureRoomVideoRuntimeState(room) {
  const sourceType = String(room?.video?.sourceType || "").toLowerCase();
  const src = String(room?.video?.src || "");
  const isAllowedFileSrc =
    src.startsWith(ROOM_VIDEO_PUBLIC_PREFIX) ||
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
      updatedAt: Number(sync.updatedAt || Date.now()),
      clientNow: Number(sync.clientNow || 0),
      clientSeq: Number(sync.clientSeq || 0)
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
    reactions: {},
    timestamp: Date.now()
  });
  room.nextMessageId += 1;
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
    removeRoomVideoAsset(currentRoom);
    rooms.delete(code);
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
}

function parseRoomPath(pathname) {
  const match = pathname.match(/^\/api\/rooms\/([A-Za-z0-9]+)(?:\/(messages|kick|host|requests|request-join|leave|video|video-sync|video-source))?$/);
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

  if (room.members.size === 0) {
    scheduleRoomDeletionIfEmpty(room);
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

function runYtDlp(args, timeoutMs = YOUTUBE_DOWNLOAD_TIMEOUT_MS) {
  const commandCandidates = [];
  if (ytDlpCommandOverride) {
    commandCandidates.push(ytDlpCommandOverride);
  } else {
    commandCandidates.push([YT_DLP_BINARY, []], ["python3", ["-m", "yt_dlp"]], ["python", ["-m", "yt_dlp"]]);
  }

  const runWithCommand = (command, baseArgs) => new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    let settled = false;
    let timer = null;
    const mergedArgs = [...baseArgs, ...args];
    let child = null;
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
      wrapped.code = error?.code === "ENOENT" ? "YT_DLP_UNAVAILABLE" : "YT_DLP_FAILED";
      finish(wrapped);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        const runError = new Error(`yt-dlp exited with code ${code}: ${stderr.trim() || stdout.trim()}`);
        runError.code = "YT_DLP_FAILED";
        finish(runError);
        return;
      }
      finish(null, { stdout, stderr });
    });
  });

  const attempt = async () => {
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
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid video stream link.") });
      return;
    }
    await serveYouTubeProxyStream(req, res, streamId);
    return;
  }

  if (req.method === "POST" && pathname === "/api/register") {
    const { username, password } = await parseBody(req);
    const cleanUser = normalizeUsername(username);
    const cleanPass = String(password || "");

    if (!isValidRegisteredUsername(cleanUser)) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ 3 أ¯طںآ½ 30 أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½/أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½.",
          "Username must be 3-30 characters and use only English letters and numbers."
        )
      });
      return;
    }
    if (!isValidPassword(cleanPass)) {
      sendJson(res, 400, {
        error: i18n(
          req,
          "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ 4 أ¯طںآ½ 128 أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.",
          "Password must be 4-128 characters."
        )
      });
      return;
    }
    if (users.has(cleanUser)) {
      sendJson(res, 409, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Username is already in use.")
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
    sendJson(res, 201, { token, username: cleanUser, announcement: getLiveSiteAnnouncement() });
    return;
  }

  if (req.method === "POST" && pathname === "/api/login") {
    const { username, password } = await parseBody(req);
    const cleanUser = normalizeUsername(username);
    const cleanPass = String(password || "");

    if (!cleanUser || !cleanPass || cleanPass.length > PASSWORD_MAX_LENGTH) {
      sendJson(res, 401, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid login credentials.")
      });
      return;
    }

    if (!users.has(cleanUser)) {
      sendJson(res, 401, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid login credentials.")
      });
      return;
    }
    const user = users.get(cleanUser);
    if (!verifyPassword(cleanUser, user, cleanPass)) {
      sendJson(res, 401, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid login credentials.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ 2 أ¯طںآ½ 30 أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Name must be 2-30 characters.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Account not found.")
      });
      return;
    }
    if (!isBanned(username)) {
      sendJson(res, 400, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "This account is not banned.")
      });
      return;
    }
    if (cleanReason.length < 8 || cleanReason.length > 500) {
      sendJson(res, 400, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ 8 أ¯طںآ½ 500 أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Appeal reason must be 8-500 characters.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid user identifier.")
      });
      return;
    }
    const username = normalizeUsername(decodedUsername);
    if (!isValidUsername(username) || !users.has(username)) {
      sendJson(res, 404, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Account not found.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.")
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { text } = await parseBody(req);
    const cleanText = String(text || "").trim();
    if (!cleanText) {
      sendJson(res, 400, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Announcement text is required.")
      });
      return;
    }
    if (cleanText.length > 500) {
      sendJson(res, 400, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ (أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ 500 أ¯طںآ½أ¯طںآ½أ¯طںآ½).", "Announcement is too long (max 500 chars).")
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, reason } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanReason = String(reason || "").trim();
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Account not found.") });
      return;
    }
    if (isSupervisor(target)) {
      sendJson(res, 400, { error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Supervisor account cannot be banned.") });
      return;
    }
    if (cleanReason.length < 3 || cleanReason.length > 300) {
      sendJson(res, 400, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ 3 أ¯طںآ½ 300 أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Ban reason must be 3-300 characters.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget, note } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    const cleanNote = String(note || "").trim();
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Account not found.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
      return;
    }
    if (!assertSupervisor(req, res, username)) {
      return;
    }
    const { username: rawTarget } = await parseBody(req);
    const target = normalizeUsername(rawTarget);
    if (!isValidUsername(target) || !users.has(target)) {
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Account not found.") });
      return;
    }
    if (isSupervisor(target)) {
      sendJson(res, 400, { error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Supervisor account cannot be deleted.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
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
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Request not found.") });
      return;
    }
    if (cleanAction !== "approve" && cleanAction !== "reject") {
      sendJson(res, 400, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid action.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }
    const isRegistered = users.has(username);
    const isGuest = guestUsers.has(username);
    if (!isRegistered && !isGuest) {
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Account not found.") });
      return;
    }
    const { displayName, avatarDataUrl } = await parseBody(req);
    const user = isRegistered ? users.get(username) : guestUsers.get(username);
    const previousRegisteredUser = isRegistered ? { ...user } : null;

    if (displayName !== undefined) {
      const cleanName = String(displayName || "").trim();
      if (cleanName.length < 2 || cleanName.length > 30) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ 2 أ¯طںآ½ 30 أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Display name must be 2 to 30 characters.")
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
            error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Avatar is invalid or too large.")
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
      sendJson(res, 401, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.") });
      return;
    }
    if (!assertNotBanned(req, res, viewer)) {
      return;
    }
    const rawTarget = pathname.slice(userProfilePrefix.length, pathname.length - userProfileSuffix.length);
    const decodedTarget = safeDecodeURIComponent(String(rawTarget || ""));
    if (decodedTarget === null) {
      sendJson(res, 400, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid user identifier.") });
      return;
    }
    const target = normalizeUsername(decodedTarget);
    if (!isValidUsername(target)) {
      sendJson(res, 400, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid user identifier.") });
      return;
    }
    if (!users.has(target) && !guestUsers.has(target)) {
      sendJson(res, 404, { error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "User not found.") });
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.")
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
          "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ (أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ 40 أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½).",
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
      name: cleanRoomName || i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½", "New Room"),
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Room not found.")
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
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Your join request is still pending.")
        });
        return;
      } else {
        sendJson(res, 403, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "You cannot join until the leader approves your request.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Unauthorized.")
      });
      return;
    }
    if (!assertNotBanned(req, res, username)) {
      return;
    }

    if (!rooms.has(roomPath.code)) {
      sendJson(res, 404, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Room not found.")
      });
      return;
    }
    const room = rooms.get(roomPath.code);
    ensureRoomRuntimeState(room);
    if (req.method === "POST" && roomPath.action === "request-join") {
      const payload = await parseBody(req);
      const cleanAction = String(payload?.action || "").trim().toLowerCase();
      if (cleanAction === "cancel") {
        room.joinRequests.delete(username);
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Join this room first.")
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
      const fromId = since;
      const messages = room.messages.filter((msg) => msg.id > fromId);
      sendJson(res, 200, { room: formatRoom(room, username), messages, announcement: getLiveSiteAnnouncement() });
      return;
    }

    if (req.method === "GET" && roomPath.action === "requests") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Only the room leader can view requests.")
        });
        return;
      }
      sendJson(res, 200, { requests: Array.from(room.joinRequests) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "requests") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Only the room leader can manage requests.")
        });
        return;
      }
      const { username: targetRaw, action } = await parseBody(req);
      const target = normalizeUsername(targetRaw);
      const cleanAction = String(action || "").trim().toLowerCase();
      if (!isValidUsername(target)) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Target username is required.")
        });
        return;
      }
      if (!room.joinRequests.has(target)) {
        sendJson(res, 404, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "No join request found for this player.")
        });
        return;
      }
      if (cleanAction !== "approve" && cleanAction !== "reject") {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid action.")
        });
        return;
      }

      room.joinRequests.delete(target);
      if (cleanAction === "approve") {
        room.approvedUsers.add(target);
        room.messageFloorByUser.set(target, 0);
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
            error: i18n(req, "بيانات التفاعل غير صالحة.", "Invalid reaction payload.")
          });
          return;
        }
        const targetMessage = findUserMessageById(room, targetMessageId);
        if (!targetMessage) {
          sendJson(res, 404, {
            error: i18n(req, "لم يتم العثور على رسالة مناسبة للتفاعل.", "No eligible message was found for reaction.")
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
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Message is empty.")
        });
        return;
      }
      if (messageText.length > 300) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Message is too long.")
        });
        return;
      }

      const normalizedReplyToMessageId = normalizeReplyToMessageId(replyToMessageId);
      if (Number.isNaN(normalizedReplyToMessageId)) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid reply identifier.")
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
              "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.",
              "You can only reply to an existing player message."
            )
          });
          return;
        }
      }

      const normalizedClientMessageId = normalizeClientMessageId(clientMessageId);
      if (clientMessageId !== undefined && !normalizedClientMessageId) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid message identifier.")
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
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½ط£آ¯ط·ع؛ط¢آ½.", "Only the room leader can remove videos.")
        });
        return;
      }
      const hadVideo = Boolean(room.video && room.video.src);
      removeRoomVideoAsset(room);
      if (hadVideo) {
        pushSystemMessage(room, "room_video_removed", { user: username });
      }
      sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Only the room leader can upload videos.")
        });
        return;
      }

      const contentType = String(req.headers["content-type"] || "").toLowerCase();
      if (contentType.includes("application/json")) {
        const { action } = await parseBody(req);
        const clearAction = String(action || "").trim().toLowerCase();
        if (clearAction === "clear" || clearAction === "remove" || clearAction === "delete") {
          const hadVideo = Boolean(room.video && room.video.src);
          removeRoomVideoAsset(room);
          if (hadVideo) {
            pushSystemMessage(room, "room_video_removed", { user: username });
          }
          sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
          return;
        }
        sendJson(res, 400, {
          code: "VIDEO_INVALID_ACTION",
          error: i18n(req, "Invalid video action.", "Invalid video action.")
        });
        return;
      }

      if (!ensureUploadsDir()) {
        sendJson(res, 503, {
          code: "VIDEO_STORAGE_UNAVAILABLE",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Server storage is unavailable right now.")
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
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Please choose a valid video file.")
        });
        return;
      }
      if (uploaded.truncated || Number(uploaded.size || 0) > ROOM_VIDEO_MAX_BYTES) {
        cleanupUploadedTemp();
        sendJson(res, 413, {
          code: "VIDEO_TOO_LARGE",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ (أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ 1GB).", "Video file is too large (max 1GB).")
        });
        return;
      }

      const mimeType = normalizeVideoMimeType(uploaded.contentType);
      if (!ROOM_VIDEO_ALLOWED_MIME_TYPES.has(mimeType)) {
        cleanupUploadedTemp();
        sendJson(res, 415, {
          code: "VIDEO_INVALID_TYPE",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½. أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ MP4 أ¯طںآ½أ¯طںآ½ WebM أ¯طںآ½أ¯طںآ½ OGG.", "Unsupported video type. Use MP4, WebM or OGG.")
        });
        return;
      }
      const cleanFileName = sanitizeUploadedFilename(uploaded.filename || "room-video");
      const extension = normalizeVideoExtension(cleanFileName, mimeType);
      const storedFileName = `room-${room.code.toLowerCase()}-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${extension}`;
      const absolutePath = path.join(getRoomVideoUploadDir(), storedFileName);
      try {
        fs.renameSync(uploaded.tempPath, absolutePath);
      } catch (_error) {
        cleanupUploadedTemp();
        sendJson(res, 503, {
          code: "VIDEO_STORAGE_UNAVAILABLE",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Server storage is unavailable right now.")
        });
        return;
      }

      const normalizedDuration = normalizeRoomVideoDuration(multipart.fields.duration);
      removeRoomVideoAsset(room);
      room.video = {
        id: randomToken(),
        sourceType: "file",
        youtubeId: "",
        src: `${ROOM_VIDEO_PUBLIC_PREFIX}${storedFileName}`,
        filename: cleanFileName,
        mimeType,
        size: Number(uploaded.size || 0),
        uploadedBy: username,
        uploadedAt: Date.now(),
        duration: normalizedDuration,
        isYouTubeCached: false
      };
      room.videoSync = {
        videoId: room.video.id,
        playing: false,
        baseTime: 0,
        playbackRate: 1,
        updatedAt: Date.now(),
        clientNow: 0,
        clientSeq: 0
      };
      pushSystemMessage(room, "room_video_set", {
        user: username,
        name: String(room.video.filename || "video")
      });

      sendJson(res, 201, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-source") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Only the room leader can set video source.")
        });
        return;
      }

      const { input } = await parseBody(req);
      const rawInput = String(input || "").trim();
      if (rawInput.length < 2) {
        sendJson(res, 400, {
          code: "YOUTUBE_INPUT_REQUIRED",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Enter a YouTube URL or search text.")
        });
        return;
      }

      let resolved = null;
      try {
        resolved = await resolveYouTubeInput(rawInput);
      } catch (_error) {
        sendJson(res, 502, {
          code: "YOUTUBE_RESOLVE_FAILED",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½. أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Could not reach YouTube right now. Try again.")
        });
        return;
      }

      if (!resolved?.videoId) {
        sendJson(res, 404, {
          code: "YOUTUBE_NOT_FOUND",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "No matching YouTube video was found.")
        });
        return;
      }
      setRoomVideoFromYouTubeEmbed(room, resolved.videoId, username, resolved.label);
      pushSystemMessage(room, "room_video_set", {
        user: username,
        name: String(room.video?.filename || resolved.label || "video")
      });
      sendJson(res, 201, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "video-sync") {
      if (room.host !== username) {
        sendJson(res, 403, {
          code: "VIDEO_HOST_ONLY",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Only the room leader can sync the video.")
        });
        return;
      }
      if (!room.video) {
        sendJson(res, 404, {
          code: "VIDEO_NOT_FOUND",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "No room video is available.")
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
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid video sync action.")
        });
        return;
      }

      const clientVideoId = String(videoId || "").trim();
      if (clientVideoId && clientVideoId !== String(room.video.id || "")) {
        sendJson(res, 409, {
          code: "VIDEO_STALE",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½. أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "This video version is outdated. Refresh room data.")
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

      sendJson(res, 200, { ok: true, room: formatRoom(room, username), video: formatRoomVideo(room) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "host") {
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
              "فقط مشرف موجود داخل الغرفة يمكنه امتلاك القيادة.",
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
        sendJson(res, 200, { ok: true, room: formatRoom(room, username) });
        return;
      }

      if (room.host !== username) {
        sendJson(res, 403, {
          code: "HOST_TRANSFER_FORBIDDEN",
          error: i18n(
            req,
            "فقط قائد الغرفة يمكنه نقل القيادة.",
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
            "يجب اختيار عضو آخر داخل الغرفة لنقل القيادة.",
            "Leader transfer target must be another member."
          )
        });
        return;
      }

      const previous = room.host;
      room.host = target;
      room.pendingHostRestore = null;
      pushSystemMessage(room, "host_changed", { user: target, previous });
      sendJson(res, 200, { ok: true, room: formatRoom(room, username) });
      return;
    }

    if (req.method === "POST" && roomPath.action === "kick") {
      if (room.host !== username) {
        sendJson(res, 403, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Only the room leader can kick players.")
        });
        return;
      }

      const { username: targetRaw } = await parseBody(req);
      const target = normalizeUsername(targetRaw);
      if (!isValidUsername(target)) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Target username is required.")
        });
        return;
      }
      if (target === room.host) {
        sendJson(res, 400, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Leader cannot kick themselves.")
        });
        return;
      }
      if (isSupervisor(target)) {
        sendJson(res, 403, {
          code: "SUPERVISOR_KICK_FORBIDDEN",
          error: i18n(req, "أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "A supervisor cannot be kicked from this room.")
        });
        return;
      }
      if (!room.members.has(target)) {
        sendJson(res, 404, {
          error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Player is not in this room.")
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
      sendJson(res, 200, { room: formatRoom(room, username) });
      return;
    }

    sendJson(res, 404, {
      error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Room endpoint not found.")
    });
    return;
  }

  sendJson(res, 404, {
    error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Endpoint not found.")
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
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Payload is too large.")
      });
      return;
    }
    if (error.message === "Invalid JSON") {
      sendJson(res, 400, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ JSON أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid JSON body.")
      });
      return;
    }
    if (error.message === "Invalid multipart form data") {
      sendJson(res, 400, {
        error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Invalid video upload payload.")
      });
      return;
    }
    sendJson(res, 500, {
      error: i18n(req, "أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½ أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½أ¯طںآ½.", "Internal server error.")
    });
  }
});

// Large room-video uploads can exceed default Node request timeout.
server.requestTimeout = 0;
server.headersTimeout = 120000;

loadUsers();
loadModeration();
ensureUploadsDir();
loadYouTubeCache();

server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
