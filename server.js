const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const MODERATION_FILE = path.join(DATA_DIR, "moderation.json");
const SUPERVISOR_USERNAME = "qasim";

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon"
};

const users = new Map();
const sessions = new Map();
const rooms = new Map();
const userLastSeen = new Map();
const bannedUsers = new Map();
const unbanRequests = new Map();

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
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
      users.set(item.username, {
        passwordHash: item.passwordHash,
        createdAt: Number(item.createdAt) || Date.now(),
        displayName: typeof item.displayName === "string" && item.displayName.trim()
          ? item.displayName.trim().slice(0, 30)
          : item.username,
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
      createdAt: info.createdAt,
      displayName: info.displayName || username,
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
      fs.writeFileSync(MODERATION_FILE, JSON.stringify({ bannedUsers: [], unbanRequests: [] }, null, 2), "utf8");
      return;
    }
    const raw = fs.readFileSync(MODERATION_FILE, "utf8");
    const parsed = JSON.parse(raw);
    const bannedList = Array.isArray(parsed?.bannedUsers) ? parsed.bannedUsers : [];
    const requestList = Array.isArray(parsed?.unbanRequests) ? parsed.unbanRequests : [];

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
  } catch (error) {
    console.error("Failed to load moderation:", error.message);
  }
}

function saveModeration() {
  try {
    ensureDataDir();
    const payload = {
      bannedUsers: Array.from(bannedUsers.values()),
      unbanRequests: Array.from(unbanRequests.values())
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

function i18n(req, ar, en) {
  if (getLang(req) === "en") {
    return en;
  }
  return looksLikeMojibake(ar) ? en : ar;
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

function hashPassword(password) {
  return crypto.createHash("sha256").update(password).digest("hex");
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
    unbanRequest: formatUnbanRequest(unbanRequests.get(String(username || "").toLowerCase()) || null)
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
  const info = users.get(username);
  if (!info) {
    return null;
  }
  return {
    username,
    displayName: info.displayName || username,
    avatarDataUrl: info.avatarDataUrl || "",
    createdAt: info.createdAt,
    isOnline: isUserOnline(username),
    isSupervisor: isSupervisor(username)
  };
}

function removeUserSessions(username) {
  sessions.forEach((sessionUser, token) => {
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
    displayName: info.displayName || username,
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

function formatRoom(room, viewer = null) {
  return {
    code: room.code,
    name: room.name,
    host: room.host,
    members: Array.from(room.members),
    createdAt: room.createdAt,
    pendingCount: room.joinRequests.size,
    hasPendingRequest: viewer ? room.joinRequests.has(viewer) : false
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

function pushSystemMessage(room, key, payload = {}) {
  room.messages.push({
    id: room.nextMessageId,
    type: "system",
    key,
    payload,
    text: "",
    timestamp: Date.now()
  });
  room.nextMessageId += 1;
}

function pushUserMessage(room, username, text) {
  room.messages.push({
    id: room.nextMessageId,
    type: "user",
    user: username,
    text,
    timestamp: Date.now()
  });
  room.nextMessageId += 1;
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
  const match = pathname.match(/^\/api\/rooms\/([A-Za-z0-9]+)(?:\/(messages|kick|requests|request-join|leave))?$/);
  if (!match) {
    return null;
  }
  return {
    code: match[1].toUpperCase(),
    action: match[2] || null
  };
}

function removeMemberFromRoom(room, username) {
  if (!room.messageFloorByUser) {
    room.messageFloorByUser = new Map();
  }
  // When a player leaves, hide old chat history for their next join.
  room.messageFloorByUser.set(username, room.nextMessageId - 1);
  room.members.delete(username);
  room.joinRequests.delete(username);

  if (room.members.size === 0) {
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
        req.destroy();
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
  if (!token || !sessions.has(token)) {
    return null;
  }
  const username = sessions.get(token);
  touchUser(username);
  return username;
}

function serveStatic(pathname, res) {
  const urlPath = pathname === "/" ? "/index.html" : pathname;
  const safePath = path.normalize(urlPath).replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(ROOT, safePath);

  if (!filePath.startsWith(ROOT)) {
    res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("403 - Forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("404 - File not found");
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    res.writeHead(200, { "Content-Type": contentType });
    res.end(data);
  });
}

async function handleApi(req, res) {
  const fullUrl = new URL(req.url, "http://localhost");
  const pathname = fullUrl.pathname;

  if (req.method === "POST" && pathname === "/api/register") {
    const { username, password } = await parseBody(req);
    const cleanUser = String(username || "").trim().toLowerCase();
    const cleanPass = String(password || "");

    if (cleanUser.length < 3 || cleanPass.length < 4) {
      sendJson(res, 400, {
        error: i18n(req, "اسم المستخدم أو كلمة المرور قصيرة جدًا.", "Username or password is too short.")
      });
      return;
    }
    if (users.has(cleanUser)) {
      sendJson(res, 409, {
        error: i18n(req, "اسم المستخدم مستخدم بالفعل.", "Username is already in use.")
      });
      return;
    }

    users.set(cleanUser, {
      passwordHash: hashPassword(cleanPass),
      createdAt: Date.now(),
      displayName: cleanUser,
      avatarDataUrl: ""
    });
    saveUsers();
    const token = randomToken();
    sessions.set(token, cleanUser);
    touchUser(cleanUser);
    sendJson(res, 201, { token, username: cleanUser });
    return;
  }

  if (req.method === "POST" && pathname === "/api/login") {
    const { username, password } = await parseBody(req);
    const cleanUser = String(username || "").trim().toLowerCase();
    const cleanPass = String(password || "");

    if (!users.has(cleanUser)) {
      sendJson(res, 401, {
        error: i18n(req, "بيانات الدخول غير صحيحة.", "Invalid login credentials.")
      });
      return;
    }
    const user = users.get(cleanUser);
    if (user.passwordHash !== hashPassword(cleanPass)) {
      sendJson(res, 401, {
        error: i18n(req, "بيانات الدخول غير صحيحة.", "Invalid login credentials.")
      });
      return;
    }
    if (!isSupervisor(cleanUser) && isBanned(cleanUser)) {
      sendBannedResponse(req, res, cleanUser);
      return;
    }

    const token = randomToken();
    sessions.set(token, cleanUser);
    touchUser(cleanUser);
    sendJson(res, 200, { token, username: cleanUser });
    return;
  }

  if (req.method === "POST" && pathname === "/api/ban-appeals") {
    const { username: rawUsername, reason } = await parseBody(req);
    const username = String(rawUsername || "").trim().toLowerCase();
    const cleanReason = String(reason || "").trim();
    if (!username || !users.has(username)) {
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
    const username = decodedUsername.trim().toLowerCase();
    if (!username || !users.has(username)) {
      sendJson(res, 404, {
        error: i18n(req, "الحساب غير موجود.", "Account not found.")
      });
      return;
    }
    sendJson(res, 200, {
      isBanned: isBanned(username),
      ban: formatBanRecord(getBanRecord(username)),
      request: formatUnbanRequest(unbanRequests.get(username) || null)
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
      banned: isBanned(username),
      ban: formatBanRecord(getBanRecord(username)),
      unbanRequest: formatUnbanRequest(unbanRequests.get(username) || null)
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
    const target = String(rawTarget || "").trim().toLowerCase();
    const cleanReason = String(reason || "").trim();
    if (!target || !users.has(target)) {
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
    const target = String(rawTarget || "").trim().toLowerCase();
    const cleanNote = String(note || "").trim();
    if (!target || !users.has(target)) {
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
    const target = String(rawTarget || "").trim().toLowerCase();
    if (!target || !users.has(target)) {
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
    const target = String(rawTarget || "").trim().toLowerCase();
    const cleanAction = String(action || "").trim().toLowerCase();
    const cleanNote = String(note || "").trim();
    if (!target || !unbanRequests.has(target)) {
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
    if (!users.has(username)) {
      sendJson(res, 404, { error: i18n(req, "الحساب غير موجود.", "Account not found.") });
      return;
    }
    const { displayName, avatarDataUrl } = await parseBody(req);
    const user = users.get(username);

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

    users.set(username, user);
    saveUsers();
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
    const target = decodedTarget.trim().toLowerCase();
    if (!target) {
      sendJson(res, 400, { error: i18n(req, "معرف المستخدم غير صالح.", "Invalid user identifier.") });
      return;
    }
    if (!users.has(target)) {
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
    let code = randomRoomCode();
    while (rooms.has(code)) {
      code = randomRoomCode();
    }

    const room = {
      code,
      name: String(roomName || "").trim() || i18n(req, "غرفة جديدة", "New Room"),
      host: username,
      pendingHostRestore: null,
      members: new Set([username]),
      approvedUsers: new Set([username]),
      joinRequests: new Set(),
      messageFloorByUser: new Map([[username, 0]]),
      messages: [],
      nextMessageId: 1,
      createdAt: Date.now()
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
      }
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
    if (!room.approvedUsers) {
      room.approvedUsers = new Set(room.members);
    }
    if (!room.messageFloorByUser) {
      room.messageFloorByUser = new Map();
    }

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

    if (!room.approvedUsers) {
      room.approvedUsers = new Set(room.members);
    }
    if (!room.messageFloorByUser) {
      room.messageFloorByUser = new Map();
    }
    if (req.method === "POST" && roomPath.action === "request-join") {
      if (room.members.has(username)) {
        sendJson(res, 200, {
          ok: true,
          status: "already_member",
          room: formatRoom(room, username)
        });
        return;
      }
      if (room.approvedUsers.has(username) || isSupervisor(username)) {
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
      const since = Number(fullUrl.searchParams.get("since") || "0");
      const floor = room.messageFloorByUser.get(username) || 0;
      const fromId = Math.max(since, floor);
      const messages = room.messages.filter((msg) => msg.id > fromId);
      sendJson(res, 200, { room: formatRoom(room, username), messages });
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
      const target = String(targetRaw || "").trim().toLowerCase();
      const cleanAction = String(action || "").trim().toLowerCase();
      if (!target) {
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
      const { text } = await parseBody(req);
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

      pushUserMessage(room, username, messageText);
      sendJson(res, 201, { ok: true });
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
      const target = String(targetRaw || "").trim().toLowerCase();
      if (!target) {
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
    serveStatic(pathname, res);
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
    sendJson(res, 500, {
      error: i18n(req, "خطأ داخلي في الخادم.", "Internal server error.")
    });
  }
});

loadUsers();
loadModeration();

server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

