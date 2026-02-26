const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const ROOT = path.resolve(__dirname, "..");
const BACKUP_FILES = [
  path.join(ROOT, "data", "users.json"),
  path.join(ROOT, "data", "moderation.json"),
  path.join(ROOT, "data", "youtube-cache.json")
];

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function randomSuffix(length = 8) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  for (let i = 0; i < length; i += 1) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

function readBackups() {
  const backups = new Map();
  for (const filePath of BACKUP_FILES) {
    if (fs.existsSync(filePath)) {
      backups.set(filePath, fs.readFileSync(filePath));
    } else {
      backups.set(filePath, null);
    }
  }
  return backups;
}

function restoreBackups(backups) {
  for (const [filePath, content] of backups.entries()) {
    if (content === null) {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }
      continue;
    }
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, content);
  }
}

async function waitForServer(baseUrl, timeoutMs = 10000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/`);
      if (response.ok) {
        return;
      }
    } catch (_error) {
      // Server is not ready yet.
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error("Server did not become ready in time.");
}

async function request(baseUrl, route, options = {}) {
  const {
    method = "GET",
    token = "",
    lang = "en",
    body = undefined,
    expectedStatus = 200
  } = options;
  const response = await fetch(`${baseUrl}${route}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-Lang": lang,
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    },
    body: body === undefined ? undefined : JSON.stringify(body)
  });

  const text = await response.text();
  const contentType = String(response.headers.get("content-type") || "");
  const parsed = contentType.includes("application/json")
    ? (text ? JSON.parse(text) : {})
    : text;

  if (response.status !== expectedStatus) {
    throw new Error(
      `Unexpected status for ${method} ${route}: got ${response.status}, expected ${expectedStatus}. Body: ${text}`
    );
  }

  return { response, data: parsed };
}

async function run() {
  const port = 3217 + Math.floor(Math.random() * 200);
  const baseUrl = `http://127.0.0.1:${port}`;
  const backups = readBackups();
  let serverProcess = null;
  let serverStdErr = "";

  try {
    serverProcess = spawn(process.execPath, ["server.js"], {
      cwd: ROOT,
      env: {
        ...process.env,
        PORT: String(port),
        YTDL_NO_DEBUG_FILE: "1",
        YTDL_NO_UPDATE: "1"
      },
      stdio: ["ignore", "pipe", "pipe"]
    });

    serverProcess.stderr.on("data", (chunk) => {
      serverStdErr += chunk.toString("utf8");
    });

    await waitForServer(baseUrl);

    const staticIndex = await fetch(`${baseUrl}/`);
    const indexHtml = await staticIndex.text();
    assert(staticIndex.status === 200, "Index page should return 200.");
    assert(indexHtml.includes("id=\"langSelect\""), "Index page should include language selector.");

    const staticRoom = await fetch(`${baseUrl}/room.html`);
    const roomHtml = await staticRoom.text();
    assert(staticRoom.status === 200, "Room page should return 200.");
    assert(roomHtml.includes("id=\"langSelect\""), "Room page should include language selector.");

    const staticGuest = await fetch(`${baseUrl}/guest.html`);
    const guestHtml = await staticGuest.text();
    assert(staticGuest.status === 200, "Guest page should return 200.");
    assert(guestHtml.includes("id=\"langSelect\""), "Guest page should include language selector.");

    const headRoom = await fetch(`${baseUrl}/room.html`, { method: "HEAD" });
    assert(headRoom.status === 200, "HEAD /room.html should return 200.");

    const userA = `u${randomSuffix(8)}`;
    const userB = `u${randomSuffix(8)}`;
    const pass = `p-${randomSuffix(10)}`;

    const registerA = await request(baseUrl, "/api/register", {
      method: "POST",
      body: { username: userA, password: pass },
      expectedStatus: 201
    });
    const tokenA = registerA.data.token;
    assert(Boolean(tokenA), "Register A should return token.");

    const registerB = await request(baseUrl, "/api/register", {
      method: "POST",
      body: { username: userB, password: pass },
      expectedStatus: 201
    });
    const tokenB = registerB.data.token;
    assert(Boolean(tokenB), "Register B should return token.");

    await request(baseUrl, "/api/login", {
      method: "POST",
      body: { username: userA, password: "wrong-password" },
      expectedStatus: 401
    });

    const meA = await request(baseUrl, "/api/me", { token: tokenA });
    assert(meA.data.username === userA, "Me endpoint should return user A.");

    const createRoom = await request(baseUrl, "/api/rooms/create", {
      method: "POST",
      token: tokenA,
      body: { roomName: "QA Smoke Room" },
      expectedStatus: 201
    });
    const roomCode = createRoom.data.room.code;
    assert(Boolean(roomCode), "Create room should return room code.");

    await request(baseUrl, `/api/rooms/${roomCode}/request-join`, {
      method: "POST",
      token: tokenB,
      expectedStatus: 202
    });

    await request(baseUrl, "/api/rooms/join", {
      method: "POST",
      token: tokenB,
      body: { code: roomCode },
      expectedStatus: 403
    });

    const pending = await request(baseUrl, `/api/rooms/${roomCode}/requests`, {
      token: tokenA
    });
    assert(Array.isArray(pending.data.requests), "Pending requests should be an array.");
    assert(pending.data.requests.includes(userB), "Pending requests should include user B.");

    await request(baseUrl, `/api/rooms/${roomCode}/requests`, {
      method: "POST",
      token: tokenA,
      body: { username: userB, action: "approve" },
      expectedStatus: 200
    });

    const joinB = await request(baseUrl, "/api/rooms/join", {
      method: "POST",
      token: tokenB,
      body: { code: roomCode },
      expectedStatus: 200
    });
    assert(Array.isArray(joinB.data.room.members), "Joined room should include members list.");
    assert(joinB.data.room.members.includes(userB), "User B should become a room member.");

    await request(baseUrl, `/api/rooms/${roomCode}/messages`, {
      method: "POST",
      token: tokenB,
      body: { text: "Smoke test message" },
      expectedStatus: 201
    });

    const msgs = await request(baseUrl, `/api/rooms/${roomCode}/messages?since=0`, {
      token: tokenA
    });
    assert(Array.isArray(msgs.data.messages), "Messages response should include a messages array.");
    assert(
      msgs.data.messages.some((msg) => msg && msg.type === "user" && msg.user === userB),
      "Host should receive user B message."
    );
    const userBMessage = msgs.data.messages.find((msg) => msg && msg.type === "user" && msg.user === userB);
    assert(Boolean(userBMessage?.id), "User B message id should be available.");

    const reactResult = await request(baseUrl, `/api/rooms/${roomCode}/messages`, {
      method: "POST",
      token: tokenA,
      body: { action: "react", messageId: userBMessage.id, emoji: "🔥" },
      expectedStatus: 200
    });
    assert(reactResult.data.ok === true, "Reaction API should return ok=true.");

    const msgsAfterReaction = await request(baseUrl, `/api/rooms/${roomCode}/messages?since=0`, {
      token: tokenB
    });
    const reactedMessage = msgsAfterReaction.data.messages.find((msg) => msg && msg.id === userBMessage.id);
    assert(
      Array.isArray(reactedMessage?.reactions?.["🔥"]) && reactedMessage.reactions["🔥"].includes(userA),
      "Reacted message should include user A in fire emoji reactions."
    );
    assert(
      msgsAfterReaction.data.messages.some(
        (msg) => msg && msg.type === "system" && msg.key === "message_reaction" && msg.payload?.messageId === userBMessage.id
      ),
      "Reaction update system event should be emitted."
    );

    await request(baseUrl, `/api/rooms/${roomCode}/kick`, {
      method: "POST",
      token: tokenA,
      body: { username: userB },
      expectedStatus: 200
    });

    await request(baseUrl, `/api/rooms/${roomCode}/messages?since=0`, {
      token: tokenB,
      expectedStatus: 403
    });

    await request(baseUrl, "/api/profile", {
      method: "PATCH",
      token: tokenA,
      body: { displayName: "SmokeUserA" },
      expectedStatus: 200
    });

    const profileA = await request(baseUrl, `/api/profile?username=${encodeURIComponent(userA)}`, {
      token: tokenA,
      expectedStatus: 200
    });
    assert(profileA.data.profile.displayName === "SmokeUserA", "Profile update should persist.");

    await request(baseUrl, "/api/admin/users", {
      token: tokenA,
      expectedStatus: 403
    });

    const guestLogin = await request(baseUrl, "/api/guest-login", {
      method: "POST",
      body: { name: "Smoke Guest" },
      expectedStatus: 200
    });
    assert(String(guestLogin.data.username || "").startsWith("guest_"), "Guest login should return guest username.");

    console.log("Smoke test passed.");
  } finally {
    if (serverProcess && !serverProcess.killed) {
      serverProcess.kill("SIGTERM");
    }
    restoreBackups(backups);
    if (serverStdErr.trim()) {
      console.log("Server stderr during smoke test:");
      console.log(serverStdErr.trim());
    }
  }
}

run().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
