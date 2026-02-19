const TOKEN_KEY = "bedna_token";
const USER_KEY = "bedna_user";
const LANG_KEY = "bedna_lang";

const roomName = document.getElementById("roomName");
const roomCode = document.getElementById("roomCode");
const roomLeader = document.getElementById("roomLeader");
const meName = document.getElementById("meName");
const playersList = document.getElementById("playersList");
const roomMembersCountText = document.getElementById("roomMembersCountText");
const pendingRequestsBox = document.getElementById("pendingRequestsBox");
const pendingRequestsTitle = document.getElementById("pendingRequestsTitle");
const pendingRequestsList = document.getElementById("pendingRequestsList");
const backToLobbyLink = document.getElementById("backToLobby");
const joinRequestModal = document.getElementById("joinRequestModal");
const joinModalTitle = document.getElementById("joinModalTitle");
const joinModalText = document.getElementById("joinModalText");
const joinModalTimer = document.getElementById("joinModalTimer");
const joinModalApprove = document.getElementById("joinModalApprove");
const joinModalReject = document.getElementById("joinModalReject");
const joinModalClose = document.getElementById("joinModalClose");
const chatMessages = document.getElementById("chatMessages");
const chatForm = document.getElementById("chatForm");
const chatInput = document.getElementById("chatInput");
const toastContainer = document.getElementById("toastContainer");
const langSelect = document.getElementById("langSelect");
const soundToggleBtn = document.getElementById("soundToggle");
const openMyProfileBtn = document.getElementById("openMyProfile");
const profileModal = document.getElementById("profileModal");
const profileModalTitle = document.getElementById("profileModalTitle");
const profileAvatar = document.getElementById("profileAvatar");
const profileUsername = document.getElementById("profileUsername");
const profileStatus = document.getElementById("profileStatus");
const profileCreatedAt = document.getElementById("profileCreatedAt");
const profileEditBox = document.getElementById("profileEditBox");
const profileDisplayNameLabel = document.getElementById("profileDisplayNameLabel");
const profileDisplayNameInput = document.getElementById("profileDisplayNameInput");
const profileAvatarLabel = document.getElementById("profileAvatarLabel");
const profileAvatarInput = document.getElementById("profileAvatarInput");
const profileSaveBtn = document.getElementById("profileSaveBtn");
const profileRemoveAvatarBtn = document.getElementById("profileRemoveAvatarBtn");
const profileCloseBtn = document.getElementById("profileCloseBtn");
const profileAdminBox = document.getElementById("profileAdminBox");
const profileBanReasonLabel = document.getElementById("profileBanReasonLabel");
const profileBanReasonInput = document.getElementById("profileBanReasonInput");
const profileBanBtn = document.getElementById("profileBanBtn");
const profileUnbanBtn = document.getElementById("profileUnbanBtn");

const query = new URLSearchParams(window.location.search);
const code = String(query.get("code") || "").trim().toUpperCase();
const encodedCode = encodeURIComponent(code);

let me = "";
let host = "";
let lastMessageId = 0;
let pollTimer = null;
let currentPendingRequests = [];
let requestModalQueue = [];
let activeRequestUser = "";
let requestModalSeconds = 0;
let requestModalTimer = null;
let hasLeftRoom = false;
let selectedAvatarDataUrl = null;
let activeProfileUsername = "";
const profileCache = new Map();
let isSupervisor = false;
let activeProfileModeration = null;

const I18N = {
  ar: {
    pageTitle: "الغرفة",
    langLabel: "اللغة",
    codeLabel: "الرمز",
    leaderLabel: "القائد",
    youLabel: "أنت",
    backToLobby: "العودة إلى اللوبي",
    chatTitle: "دردشة الغرفة",
    chatPlaceholder: "اكتب رسالتك...",
    sendBtn: "إرسال",
    playersTitle: "اللاعبون",
    roomMembersCount: "داخل الغرفة الآن: {count}",
    pendingRequestsTitle: "طلبات الانضمام",
    pendingEmpty: "لا توجد طلبات حالياً.",
    approveBtn: "قبول",
    rejectBtn: "رفض",
    closeBtn: "إغلاق",
    joinModalTitle: "طلب انضمام",
    joinModalPrompt: "{user} يريد الانضمام إلى الغرفة.",
    joinModalTimer: "إغلاق تلقائي خلال {seconds} ثانية",
    playerLeaderSuffix: "القائد",
    kickBtn: "طرد",
    systemName: "النظام",
    toastRequestFailed: "فشل الطلب.",
    toastKickedOk: "تم طرد {player} من الغرفة.",
    toastJoinApproved: "تم قبول {user}.",
    toastJoinRejected: "تم رفض {user}.",
    toastLoginFirst: "يرجى تسجيل الدخول أولًا.",
    toastInvalidCode: "رمز الغرفة غير صالح.",
    toastKickedOut: "تم طردك من هذه الغرفة ويمكنك إرسال طلب انضمام جديد.",
    toastAccountBanned: "تم حظر حسابك من الموقع.",
    sysCreated: "{user} أنشأ الغرفة.",
    sysJoined: "{user} انضم إلى الغرفة.",
    sysKicked: "تم طرد {user} بواسطة القائد.",
    sysLeft: "{user} غادر الغرفة.",
    sysHostChanged: "{user} أصبح القائد الجديد.",
    supervisorBadge: "مشرف",
    profileBtn: "ملفي",
    profileTitle: "الملف الشخصي",
    profileOnline: "الحالة: متصل الآن",
    profileOffline: "الحالة: غير متصل",
    profileCreated: "تاريخ إنشاء الحساب: {date}",
    profileDisplayName: "الاسم الظاهر",
    profileAvatar: "الصورة الشخصية",
    profileSave: "حفظ",
    profileRemoveAvatar: "إزالة الصورة",
    profileSaved: "تم حفظ الملف الشخصي.",
    profileOpenFail: "تعذر تحميل الملف الشخصي.",
    profileNameInvalid: "الاسم الظاهر يجب أن يكون بين 2 و 30 حرفًا.",
    supervisorBanReason: "سبب الحظر",
    supervisorBanBtn: "حظر الحساب",
    supervisorUnbanBtn: "إزالة الحظر",
    supervisorNeedReason: "اكتب سبب الحظر (3 أحرف على الأقل).",
    supervisorBanDone: "تم حظر الحساب.",
    supervisorUnbanDone: "تم رفع الحظر.",
    soundOn: "الصوت: تشغيل",
    soundOff: "الصوت: إيقاف"
  },
  en: {
    pageTitle: "Room",
    langLabel: "Language",
    codeLabel: "Code",
    leaderLabel: "Leader",
    youLabel: "You",
    backToLobby: "Back to Lobby",
    chatTitle: "Room Chat",
    chatPlaceholder: "Type your message...",
    sendBtn: "Send",
    playersTitle: "Players",
    roomMembersCount: "Inside room now: {count}",
    pendingRequestsTitle: "Join Requests",
    pendingEmpty: "No pending requests.",
    approveBtn: "Approve",
    rejectBtn: "Reject",
    closeBtn: "Close",
    joinModalTitle: "Join Request",
    joinModalPrompt: "{user} wants to join this room.",
    joinModalTimer: "Auto close in {seconds}s",
    playerLeaderSuffix: "Leader",
    kickBtn: "Kick",
    systemName: "System",
    toastRequestFailed: "Request failed.",
    toastKickedOk: "{player} was removed from the room.",
    toastJoinApproved: "{user} was approved.",
    toastJoinRejected: "{user} was rejected.",
    toastLoginFirst: "Please login first.",
    toastInvalidCode: "Invalid room code.",
    toastKickedOut: "You were kicked from this room and can request to join again.",
    toastAccountBanned: "Your account was banned from the site.",
    sysCreated: "{user} created the room.",
    sysJoined: "{user} joined the room.",
    sysKicked: "{user} was kicked by the leader.",
    sysLeft: "{user} left the room.",
    sysHostChanged: "{user} is now the leader.",
    supervisorBadge: "Supervisor",
    profileBtn: "My Profile",
    profileTitle: "Profile",
    profileOnline: "Status: Online",
    profileOffline: "Status: Offline",
    profileCreated: "Account created: {date}",
    profileDisplayName: "Display Name",
    profileAvatar: "Profile Image",
    profileSave: "Save",
    profileRemoveAvatar: "Remove Image",
    profileSaved: "Profile saved.",
    profileOpenFail: "Failed to load profile.",
    profileNameInvalid: "Display name must be 2 to 30 characters.",
    supervisorBanReason: "Ban Reason",
    supervisorBanBtn: "Ban Account",
    supervisorUnbanBtn: "Unban Account",
    supervisorNeedReason: "Please write a ban reason (at least 3 chars).",
    supervisorBanDone: "Account banned.",
    supervisorUnbanDone: "Account unbanned.",
    soundOn: "SFX: ON",
    soundOff: "SFX: OFF"
  }
};

function getLang() {
  const saved = localStorage.getItem(LANG_KEY);
  return saved === "en" ? "en" : "ar";
}

function setLang(lang) {
  const next = lang === "en" ? "en" : "ar";
  localStorage.setItem(LANG_KEY, next);
  document.documentElement.lang = next;
  document.documentElement.dir = next === "ar" ? "rtl" : "ltr";
  langSelect.value = next;
  applyTranslations();
  if (code && me) {
    chatMessages.innerHTML = "";
    lastMessageId = 0;
    refreshMessages();
  }
}

function t(key) {
  const lang = getLang();
  const value = I18N[lang][key];
  if (value === undefined) {
    return I18N.en[key] || key;
  }
  return value;
}

function fmt(template, vars) {
  return template.replace(/\{(\w+)\}/g, (_, k) => (vars[k] !== undefined ? vars[k] : `{${k}}`));
}

function sfx(name) {
  if (window.BednaSound && typeof window.BednaSound.play === "function") {
    window.BednaSound.play(name);
  }
}

function renderSoundToggle() {
  if (!soundToggleBtn || !window.BednaSound) {
    return;
  }
  const enabled = window.BednaSound.isEnabled();
  soundToggleBtn.textContent = enabled ? t("soundOn") : t("soundOff");
  soundToggleBtn.classList.toggle("off", !enabled);
}

function formatDate(ts) {
  const value = Number(ts || 0);
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat(getLang() === "ar" ? "ar" : "en", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(new Date(value));
}

function formatChatTime(ts) {
  const value = Number(ts || 0);
  if (!value) {
    return "";
  }
  return new Intl.DateTimeFormat(getLang() === "ar" ? "ar" : "en", {
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(value));
}

function displayNameFor(username) {
  const profile = profileCache.get(username);
  return profile?.displayName || username;
}

function isSupervisorUser(username) {
  if (!username) {
    return false;
  }
  if (username === me && isSupervisor) {
    return true;
  }
  return Boolean(profileCache.get(username)?.isSupervisor);
}

function createSupervisorBadge() {
  const badge = document.createElement("span");
  badge.className = "role-badge supervisor-badge";
  badge.textContent = t("supervisorBadge");
  return badge;
}

function avatarInitial(username) {
  const profile = profileCache.get(username);
  const source = (profile?.displayName || username || "?").trim();
  return source.slice(0, 1).toUpperCase();
}

function createAvatarButton(username) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "avatar-btn";
  if (isSupervisorUser(username)) {
    btn.classList.add("supervisor-avatar");
  }
  btn.title = displayNameFor(username);
  btn.addEventListener("click", () => openProfileModal(username));

  const avatar = profileCache.get(username)?.avatarDataUrl || "";
  if (avatar) {
    const img = document.createElement("img");
    img.src = avatar;
    img.alt = username;
    btn.appendChild(img);
    return btn;
  }

  const fallback = document.createElement("span");
  fallback.className = "avatar-fallback";
  fallback.textContent = avatarInitial(username);
  btn.appendChild(fallback);
  return btn;
}

async function fetchProfile(username, force = false) {
  if (!force && profileCache.has(username)) {
    return profileCache.get(username);
  }
  const result = await api(`/api/users/${encodeURIComponent(username)}/profile`);
  if (result?.profile) {
    profileCache.set(username, result.profile);
    return { profile: result.profile, moderation: result.moderation || null, isSelf: Boolean(result.isSelf) };
  }
  return { profile: null, moderation: null, isSelf: false };
}

async function preloadProfiles(usernames) {
  const unique = Array.from(new Set(usernames.filter(Boolean)));
  const missing = unique.filter((username) => !profileCache.has(username));
  if (missing.length === 0) {
    return;
  }
  await Promise.all(
    missing.map(async (username) => {
      try {
        await fetchProfile(username);
      } catch (_) {
        // Ignore profile failures and keep graceful fallback.
      }
    })
  );
}

function closeProfileModal() {
  activeProfileUsername = "";
  selectedAvatarDataUrl = null;
  profileAvatarInput.value = "";
  profileModal.classList.remove("is-supervisor-profile");
  profileUsername.classList.remove("is-supervisor");
  profileModal.classList.add("hidden");
}

async function openProfileModal(username) {
  try {
    const result = await fetchProfile(username, true);
    const profile = result.profile;
    if (!profile) {
      showToast(t("profileOpenFail"));
      return;
    }
    activeProfileUsername = profile.username;
    activeProfileModeration = result.moderation || null;
    const isSelf = profile.username === me;
    profileModalTitle.textContent = t("profileTitle");
    profileModal.classList.toggle("is-supervisor-profile", Boolean(profile.isSupervisor));
    profileUsername.classList.toggle("is-supervisor", Boolean(profile.isSupervisor));
    profileUsername.textContent = "";
    const profileIdentity = document.createElement("span");
    profileIdentity.textContent = `@${profile.username} • ${profile.displayName || profile.username}`;
    profileUsername.appendChild(profileIdentity);
    if (profile.isSupervisor) {
      profileUsername.appendChild(createSupervisorBadge());
    }
    const profileStatusText = profile.isOnline ? t("profileOnline") : t("profileOffline");
    profileStatus.textContent = profile.isSupervisor
      ? `${profileStatusText} • ${t("supervisorBadge")}`
      : profileStatusText;
    profileCreatedAt.textContent = fmt(t("profileCreated"), { date: formatDate(profile.createdAt) });
    profileAvatar.src = profile.avatarDataUrl || "";
    if (!profile.avatarDataUrl) {
      profileAvatar.removeAttribute("src");
    }
    profileEditBox.classList.toggle("hidden", !isSelf);
    if (isSelf) {
      profileDisplayNameInput.value = profile.displayName || profile.username;
      selectedAvatarDataUrl = profile.avatarDataUrl || null;
    }
    const canModerate = isSupervisor && profile.username !== me;
    profileAdminBox.classList.toggle("hidden", !canModerate);
    if (canModerate) {
      profileBanReasonInput.value = "";
      const isTargetBanned = Boolean(activeProfileModeration?.isBanned);
      profileBanBtn.disabled = isTargetBanned;
      profileUnbanBtn.disabled = !isTargetBanned;
    }
    sfx("modal");
    profileModal.classList.remove("hidden");
  } catch (error) {
    showToast(error.message || t("profileOpenFail"));
  }
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Failed to read image."));
    reader.readAsDataURL(file);
  });
}

async function saveMyProfile() {
  if (!me) {
    return;
  }
  const displayName = String(profileDisplayNameInput.value || "").trim();
  if (displayName.length < 2 || displayName.length > 30) {
    showToast(t("profileNameInvalid"));
    return;
  }
  const body = { displayName };
  if (selectedAvatarDataUrl !== null) {
    body.avatarDataUrl = selectedAvatarDataUrl;
  }
  const result = await api("/api/profile", { method: "PATCH", body });
  if (result?.profile) {
    profileCache.set(result.profile.username, result.profile);
    meName.textContent = result.profile.displayName || result.profile.username;
  }
  showToast(t("profileSaved"), "success");
  await refreshMessages();
}

async function supervisorBanTarget() {
  if (!isSupervisor || !activeProfileUsername || activeProfileUsername === me) {
    return;
  }
  const reason = String(profileBanReasonInput.value || "").trim();
  if (reason.length < 3) {
    showToast(t("supervisorNeedReason"));
    return;
  }
  await api("/api/admin/ban-user", {
    method: "POST",
    body: {
      username: activeProfileUsername,
      reason
    }
  });
  showToast(t("supervisorBanDone"), "success");
  await openProfileModal(activeProfileUsername);
}

async function supervisorUnbanTarget() {
  if (!isSupervisor || !activeProfileUsername || activeProfileUsername === me) {
    return;
  }
  await api("/api/admin/unban-user", {
    method: "POST",
    body: {
      username: activeProfileUsername,
      note: "Unbanned by supervisor"
    }
  });
  showToast(t("supervisorUnbanDone"), "success");
  await openProfileModal(activeProfileUsername);
}

function showToast(text, type = "error") {
  if (!text) {
    return;
  }
  sfx(type === "success" ? "success" : "error");
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = text;
  toastContainer.appendChild(toast);
  setTimeout(() => {
    toast.classList.add("hide");
    setTimeout(() => toast.remove(), 300);
  }, 2600);
}

function renderRoomMembersCount(count = 0) {
  roomMembersCountText.textContent = fmt(t("roomMembersCount"), { count: Number(count || 0) });
}

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function leaveCurrentRoom({ keepalive = false } = {}) {
  if (hasLeftRoom || !code) {
    return Promise.resolve();
  }
  const token = getToken();
  if (!token) {
    return Promise.resolve();
  }
  hasLeftRoom = true;
  return fetch(`/api/rooms/${encodedCode}/leave`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Lang": getLang(),
      Authorization: `Bearer ${token}`
    },
    body: "{}",
    keepalive
  }).catch(() => {});
}

function redirectToHome(message = "") {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  closeRequestModal();
  const suffix = message ? `?msg=${encodeURIComponent(message)}` : "";
  window.location.href = `/${suffix}`;
}

async function api(pathname, options = {}) {
  const token = getToken();
  const headers = {
    "Content-Type": "application/json",
    "X-Lang": getLang(),
    ...(options.headers || {})
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(pathname, {
    method: options.method || "GET",
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data.error || t("toastRequestFailed"));
    error.status = response.status;
    error.code = data.code || "";
    error.data = data;
    throw error;
  }
  return data;
}

function systemMessageText(message) {
  if (message.key === "room_created") {
    return fmt(t("sysCreated"), { user: message.payload?.user || "" });
  }
  if (message.key === "room_joined") {
    return fmt(t("sysJoined"), { user: message.payload?.user || "" });
  }
  if (message.key === "player_kicked") {
    return fmt(t("sysKicked"), { user: message.payload?.user || "" });
  }
  if (message.key === "room_left") {
    return fmt(t("sysLeft"), { user: message.payload?.user || "" });
  }
  if (message.key === "host_changed") {
    return fmt(t("sysHostChanged"), { user: message.payload?.user || "" });
  }
  return message.text || "";
}

let currentMembers = [];

function renderPendingRequests(requests) {
  currentPendingRequests = requests.slice();
  pendingRequestsList.innerHTML = "";

  if (me !== host) {
    pendingRequestsBox.classList.add("hidden");
    return;
  }
  pendingRequestsBox.classList.remove("hidden");

  if (requests.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("pendingEmpty");
    pendingRequestsList.appendChild(empty);
    return;
  }

  requests.forEach((requestUser) => {
    const item = document.createElement("div");
    item.className = "request-item";

    const name = document.createElement("span");
    name.textContent = requestUser;
    item.appendChild(name);

    const actions = document.createElement("div");
    actions.className = "request-actions";

    const approveBtn = document.createElement("button");
    approveBtn.type = "button";
    approveBtn.className = "btn btn-approve";
    approveBtn.textContent = t("approveBtn");
    approveBtn.addEventListener("click", () => handleRequestDecision(requestUser, "approve"));
    actions.appendChild(approveBtn);

    const rejectBtn = document.createElement("button");
    rejectBtn.type = "button";
    rejectBtn.className = "btn btn-reject";
    rejectBtn.textContent = t("rejectBtn");
    rejectBtn.addEventListener("click", () => handleRequestDecision(requestUser, "reject"));
    actions.appendChild(rejectBtn);

    item.appendChild(actions);
    pendingRequestsList.appendChild(item);
  });
}

function closeRequestModal() {
  joinRequestModal.classList.add("hidden");
  activeRequestUser = "";
  requestModalSeconds = 0;
  if (requestModalTimer) {
    clearInterval(requestModalTimer);
    requestModalTimer = null;
  }
}

function updateModalTimerText() {
  joinModalTimer.textContent = fmt(t("joinModalTimer"), { seconds: requestModalSeconds });
}

async function dismissActiveModalRequest(action, silentToast = false) {
  const target = activeRequestUser;
  if (!target) {
    closeRequestModal();
    return;
  }
  closeRequestModal();
  await handleRequestDecision(target, action, { silentToast });
}

function openRequestModal(username) {
  activeRequestUser = username;
  requestModalSeconds = 60;
  joinModalTitle.textContent = t("joinModalTitle");
  joinModalText.textContent = fmt(t("joinModalPrompt"), { user: username });
  updateModalTimerText();
  sfx("modal");
  joinRequestModal.classList.remove("hidden");

  if (requestModalTimer) {
    clearInterval(requestModalTimer);
  }
  requestModalTimer = setInterval(() => {
    requestModalSeconds -= 1;
    if (requestModalSeconds <= 0) {
      dismissActiveModalRequest("reject", true);
      return;
    }
    updateModalTimerText();
  }, 1000);
}

function maybeOpenNextRequestModal() {
  if (activeRequestUser || me !== host) {
    return;
  }
  const nextUser = requestModalQueue.shift();
  if (!nextUser) {
    return;
  }
  openRequestModal(nextUser);
}

function syncRequestModalQueue(requests) {
  if (me !== host) {
    requestModalQueue = [];
    closeRequestModal();
    return;
  }
  const activeSet = new Set(requests);
  requestModalQueue = requestModalQueue.filter((user) => activeSet.has(user));
  if (activeRequestUser && !activeSet.has(activeRequestUser)) {
    closeRequestModal();
  }
  requests.forEach((user) => {
    if (user !== activeRequestUser && !requestModalQueue.includes(user)) {
      requestModalQueue.push(user);
    }
  });
  maybeOpenNextRequestModal();
}

async function refreshPendingRequests() {
  if (me !== host) {
    renderPendingRequests([]);
    syncRequestModalQueue([]);
    return;
  }
  try {
    const result = await api(`/api/rooms/${encodedCode}/requests`);
    const requests = result.requests || [];
    renderPendingRequests(requests);
    syncRequestModalQueue(requests);
  } catch (error) {
    showToast(error.message);
  }
}

async function handleRequestDecision(username, action, options = {}) {
  const { silentToast = false } = options;
  try {
    await api(`/api/rooms/${encodedCode}/requests`, {
      method: "POST",
      body: { username, action }
    });
    if (!silentToast && action === "approve") {
      showToast(fmt(t("toastJoinApproved"), { user: username }), "success");
    } else if (!silentToast) {
      showToast(fmt(t("toastJoinRejected"), { user: username }), "success");
    }
    await refreshMessages();
  } catch (error) {
    showToast(error.message);
  }
}

function renderPlayers(members) {
  currentMembers = members.slice();
  playersList.innerHTML = "";
  members.forEach((player) => {
    const item = document.createElement("li");
    item.className = "player-item";
    const playerIsSupervisor = isSupervisorUser(player);
    if (playerIsSupervisor) {
      item.classList.add("is-supervisor");
    }

    const info = document.createElement("div");
    info.className = "player-info";
    info.appendChild(createAvatarButton(player));

    const name = document.createElement("span");
    name.className = "player-name";
    name.dataset.user = player;
    const baseName = displayNameFor(player);
    name.textContent = player === host ? `${baseName} (${t("playerLeaderSuffix")})` : baseName;
    if (playerIsSupervisor) {
      name.appendChild(createSupervisorBadge());
    }
    info.appendChild(name);
    item.appendChild(info);

    if (me === host && player !== host) {
      const kickBtn = document.createElement("button");
      kickBtn.className = "kick-btn";
      kickBtn.type = "button";
      kickBtn.textContent = t("kickBtn");
      kickBtn.addEventListener("click", async () => {
        try {
          await api(`/api/rooms/${encodedCode}/kick`, {
            method: "POST",
            body: { username: player }
          });
          showToast(fmt(t("toastKickedOk"), { player }), "success");
          await refreshMessages();
        } catch (error) {
          showToast(error.message);
        }
      });
      item.appendChild(kickBtn);
    }

    playersList.appendChild(item);
  });
}

function appendMessage(message) {
  const lastRow = chatMessages.lastElementChild;
  if (
    message.type === "user" &&
    lastRow &&
    lastRow.dataset.messageType === "user" &&
    lastRow.dataset.user === message.user
  ) {
    const lastBody = lastRow.querySelector(".chat-body");
    if (lastBody) {
      const extraLine = document.createElement("div");
      extraLine.className = "chat-body-line";
      extraLine.textContent = message.text;
      lastBody.appendChild(extraLine);
      chatMessages.scrollTop = chatMessages.scrollHeight;
      return;
    }
  }

  const row = document.createElement("div");
  const messageIsSupervisor = message.type === "user" && isSupervisorUser(message.user);
  if (message.type === "system") {
    row.className = "chat-row system";
  } else if (message.user === me) {
    row.className = "chat-row user own";
  } else {
    row.className = "chat-row user other";
  }
  if (messageIsSupervisor) {
    row.classList.add("is-supervisor");
  }
  row.dataset.messageType = message.type;
  row.dataset.user = message.user || "";

  const avatar = message.type !== "system" ? createAvatarButton(message.user) : null;

  if (avatar && message.user !== me) {
    row.appendChild(avatar);
  }

  const content = document.createElement("div");
  content.className = "chat-content";

  const meta = document.createElement("div");
  meta.className = "chat-meta";

  const head = document.createElement("span");
  head.className = "chat-head";
  if (message.type === "system") {
    head.textContent = t("systemName");
  } else {
    head.textContent = displayNameFor(message.user);
    if (messageIsSupervisor) {
      head.appendChild(createSupervisorBadge());
    }
  }

  const time = document.createElement("span");
  time.className = "chat-time";
  time.textContent = formatChatTime(message.timestamp);
  meta.appendChild(head);
  meta.appendChild(time);

  const body = document.createElement("div");
  body.className = "chat-body";
  if (message.type === "system") {
    body.textContent = systemMessageText(message);
  } else {
    const firstLine = document.createElement("div");
    firstLine.className = "chat-body-line";
    firstLine.textContent = message.text;
    body.appendChild(firstLine);
  }

  content.appendChild(meta);
  content.appendChild(body);
  row.appendChild(content);
  if (avatar && message.user === me) {
    row.appendChild(avatar);
  }
  chatMessages.appendChild(row);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function renderRoomInfo(room) {
  host = room.host;
  roomName.textContent = room.name;
  roomCode.textContent = room.code;
  const leaderText = displayNameFor(room.host);
  roomLeader.textContent = isSupervisorUser(room.host) ? `${leaderText} (${t("supervisorBadge")})` : leaderText;
  const myText = displayNameFor(me);
  meName.textContent = isSupervisorUser(me) ? `${myText} (${t("supervisorBadge")})` : myText;
  renderRoomMembersCount((room.members || []).length);
  renderPlayers(room.members);
}

async function refreshMessages() {
  try {
    const result = await api(`/api/rooms/${encodedCode}/messages?since=${lastMessageId}`);
    const members = Array.isArray(result.room?.members) ? result.room.members : [];
    const messageUsers = (result.messages || [])
      .filter((message) => message.type === "user")
      .map((message) => message.user);
    await preloadProfiles([result.room?.host, ...members, ...messageUsers, me]);
    renderRoomInfo(result.room);
    await refreshPendingRequests();
    let hasIncomingUserMessage = false;
    result.messages.forEach((message) => {
      if (message.type === "user" && message.user && message.user !== me) {
        hasIncomingUserMessage = true;
      }
      appendMessage(message);
      lastMessageId = Math.max(lastMessageId, message.id);
    });
    if (hasIncomingUserMessage) {
      sfx("message");
    }
  } catch (error) {
    if (error.code === "ACCOUNT_BANNED") {
      sfx("ban");
      redirectToHome(t("toastAccountBanned"));
      return;
    }
    if (error.status === 401) {
      redirectToHome(t("toastLoginFirst"));
      return;
    }
    if (error.status === 403) {
      redirectToHome(t("toastKickedOut"));
      return;
    }
    showToast(error.message);
  }
}

function applyTranslations() {
  document.title = t("pageTitle");
  document.getElementById("langLabel").textContent = t("langLabel");
  renderSoundToggle();
  document.getElementById("codeLabel").textContent = t("codeLabel");
  document.getElementById("leaderLabel").textContent = t("leaderLabel");
  document.getElementById("youLabel").textContent = t("youLabel");
  document.getElementById("backToLobby").textContent = t("backToLobby");
  document.getElementById("chatTitle").textContent = t("chatTitle");
  document.getElementById("chatInput").placeholder = t("chatPlaceholder");
  document.getElementById("sendBtn").textContent = t("sendBtn");
  document.getElementById("playersTitle").textContent = t("playersTitle");
  renderRoomMembersCount(currentMembers.length);
  openMyProfileBtn.textContent = t("profileBtn");
  profileModalTitle.textContent = t("profileTitle");
  profileDisplayNameLabel.textContent = t("profileDisplayName");
  profileAvatarLabel.textContent = t("profileAvatar");
  profileSaveBtn.textContent = t("profileSave");
  profileRemoveAvatarBtn.textContent = t("profileRemoveAvatar");
  profileBanReasonLabel.textContent = t("supervisorBanReason");
  profileBanBtn.textContent = t("supervisorBanBtn");
  profileUnbanBtn.textContent = t("supervisorUnbanBtn");
  profileCloseBtn.textContent = t("closeBtn");
  pendingRequestsTitle.textContent = t("pendingRequestsTitle");
  joinModalTitle.textContent = t("joinModalTitle");
  joinModalApprove.textContent = t("approveBtn");
  joinModalReject.textContent = t("rejectBtn");
  joinModalClose.textContent = t("closeBtn");
  if (activeRequestUser) {
    joinModalText.textContent = fmt(t("joinModalPrompt"), { user: activeRequestUser });
    updateModalTimerText();
  }
  if (activeProfileUsername) {
    openProfileModal(activeProfileUsername);
  }
  renderPlayers(currentMembers);
  renderPendingRequests(currentPendingRequests);
}

langSelect.addEventListener("change", (event) => {
  sfx("click");
  setLang(event.target.value);
});

if (soundToggleBtn) {
  soundToggleBtn.addEventListener("click", () => {
    if (!window.BednaSound) {
      return;
    }
    const wasEnabled = window.BednaSound.isEnabled();
    const next = window.BednaSound.toggle();
    if (!wasEnabled && next) {
      sfx("notify");
    }
    renderSoundToggle();
  });
}

joinModalApprove.addEventListener("click", () => {
  sfx("click");
  dismissActiveModalRequest("approve");
});

joinModalReject.addEventListener("click", () => {
  sfx("click");
  dismissActiveModalRequest("reject");
});

joinModalClose.addEventListener("click", () => {
  sfx("click");
  dismissActiveModalRequest("reject", true);
});

openMyProfileBtn.addEventListener("click", () => {
  sfx("click");
  openProfileModal(me);
});

profileCloseBtn.addEventListener("click", () => {
  sfx("click");
  closeProfileModal();
});

profileModal.addEventListener("click", (event) => {
  if (event.target === profileModal) {
    closeProfileModal();
  }
});

profileAvatarInput.addEventListener("change", async (event) => {
  const file = event.target.files?.[0];
  if (!file) {
    return;
  }
  try {
    const dataUrl = await readFileAsDataUrl(file);
    selectedAvatarDataUrl = dataUrl;
    profileAvatar.src = dataUrl;
  } catch (error) {
    showToast(error.message);
  }
});

profileRemoveAvatarBtn.addEventListener("click", () => {
  sfx("click");
  selectedAvatarDataUrl = "";
  profileAvatar.removeAttribute("src");
  profileAvatarInput.value = "";
});

profileSaveBtn.addEventListener("click", async () => {
  try {
    await saveMyProfile();
    await openProfileModal(me);
  } catch (error) {
    showToast(error.message);
  }
});

profileBanBtn.addEventListener("click", async () => {
  try {
    await supervisorBanTarget();
  } catch (error) {
    showToast(error.message);
  }
});

profileUnbanBtn.addEventListener("click", async () => {
  try {
    await supervisorUnbanTarget();
  } catch (error) {
    showToast(error.message);
  }
});

backToLobbyLink.addEventListener("click", async (event) => {
  event.preventDefault();
  sfx("leave");
  await leaveCurrentRoom();
  window.location.href = "/";
});

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const text = String(chatInput.value || "").trim();
  if (!text) {
    return;
  }

  try {
    await api(`/api/rooms/${encodedCode}/messages`, {
      method: "POST",
      body: { text }
    });
    sfx("send");
    chatInput.value = "";
    await refreshMessages();
  } catch (error) {
    showToast(error.message);
  }
});

async function bootRoom() {
  if (!getToken()) {
    redirectToHome(t("toastLoginFirst"));
    return;
  }
  if (!code) {
    redirectToHome(t("toastInvalidCode"));
    return;
  }

  try {
    const meData = await api("/api/me");
    me = meData.username;
    isSupervisor = Boolean(meData.isSupervisor);
    localStorage.setItem(USER_KEY, me);
    try {
      const profileResult = await api("/api/profile");
      if (profileResult?.profile) {
        profileCache.set(me, profileResult.profile);
      }
    } catch (_) {
      // continue even if profile API fails
    }

    const joinData = await api("/api/rooms/join", {
      method: "POST",
      body: { code }
    });
    sfx("join");
    renderRoomInfo(joinData.room);
    await refreshMessages();
    pollTimer = setInterval(refreshMessages, 1500);
  } catch (error) {
    redirectToHome(error.message);
  }
}

window.addEventListener("beforeunload", () => {
  leaveCurrentRoom({ keepalive: true });
  if (pollTimer) {
    clearInterval(pollTimer);
  }
  closeRequestModal();
});

setLang(getLang());
bootRoom();


