const splash = document.getElementById("splash");
const app = document.getElementById("app");

const authSection = document.getElementById("authSection");
const lobbySection = document.getElementById("lobbySection");
const bannedSection = document.getElementById("bannedSection");
const tabs = document.querySelectorAll(".tab");
const authForm = document.getElementById("authForm");
const authBtn = document.getElementById("authBtn");
const logoutBtn = document.getElementById("logoutBtn");
const currentUser = document.getElementById("currentUser");
const siteStatsText = document.getElementById("siteStatsText");
const langSelect = document.getElementById("langSelect");
const soundToggleBtn = document.getElementById("soundToggle");

const createRoomForm = document.getElementById("createRoomForm");
const joinRoomForm = document.getElementById("joinRoomForm");
const refreshRoomsBtn = document.getElementById("refreshRoomsBtn");
const publicRoomsList = document.getElementById("publicRoomsList");
const supervisorMenuToggleBtn = document.getElementById("supervisorMenuToggle");
const supervisorMenuCloseBtn = document.getElementById("supervisorMenuCloseBtn");
const supervisorSidebar = document.getElementById("supervisorSidebar");
const supervisorSidebarOverlay = document.getElementById("supervisorSidebarOverlay");
const supervisorSidebarTitle = document.getElementById("supervisorSidebarTitle");
const supervisorTabAnnouncement = document.getElementById("supervisorTabAnnouncement");
const supervisorTabUsers = document.getElementById("supervisorTabUsers");
const supervisorTabAppeals = document.getElementById("supervisorTabAppeals");
const supervisorAppealsSection = document.getElementById("supervisorAppealsSection");
const supervisorAppealsTitle = document.getElementById("supervisorAppealsTitle");
const supervisorAppealsRefreshBtn = document.getElementById("supervisorAppealsRefreshBtn");
const supervisorAppealsDesc = document.getElementById("supervisorAppealsDesc");
const supervisorAppealsList = document.getElementById("supervisorAppealsList");
const supervisorAnnouncementSection = document.getElementById("supervisorAnnouncementSection");
const supervisorAnnouncementTitle = document.getElementById("supervisorAnnouncementTitle");
const supervisorAnnouncementDesc = document.getElementById("supervisorAnnouncementDesc");
const supervisorAnnouncementInput = document.getElementById("supervisorAnnouncementInput");
const supervisorAnnouncementSendBtn = document.getElementById("supervisorAnnouncementSendBtn");
const supervisorUsersSection = document.getElementById("supervisorUsersSection");
const supervisorUsersTitle = document.getElementById("supervisorUsersTitle");
const supervisorUsersRefreshBtn = document.getElementById("supervisorUsersRefreshBtn");
const supervisorUsersDesc = document.getElementById("supervisorUsersDesc");
const supervisorUsersList = document.getElementById("supervisorUsersList");
const toastContainer = document.getElementById("toastContainer");
const bannedTitle = document.getElementById("bannedTitle");
const bannedDesc = document.getElementById("bannedDesc");
const bannedReasonText = document.getElementById("bannedReasonText");
const bannedDateText = document.getElementById("bannedDateText");
const appealForm = document.getElementById("appealForm");
const appealReason = document.getElementById("appealReason");
const appealReasonLabel = document.getElementById("appealReasonLabel");
const appealSubmitBtn = document.getElementById("appealSubmitBtn");
const bannedLogoutBtn = document.getElementById("bannedLogoutBtn");

const TOKEN_KEY = "bedna_token";
const USER_KEY = "bedna_user";
const LANG_KEY = "bedna_lang";
const PENDING_JOIN_KEY = "bedna_pending_join_room";
const ANNOUNCEMENT_SEEN_KEY = "bedna_seen_announcement_id";
let mode = "register";
let lobbyPollTimer = null;
let cachedRooms = [];
let cachedAppeals = [];
let cachedSupervisorUsers = [];
let cachedSiteStats = { onlineUsers: 0, usersInRooms: 0 };
let bannedUsername = "";
let currentBanInfo = null;
let isSupervisor = false;
let isSupervisorMenuOpen = false;
let activeSupervisorTab = "announcement";
let banStatusPollTimer = null;
let accountTransitionBusy = false;
let accountStateOverlay = null;
let globalAnnouncementOverlay = null;
let globalAnnouncementTitle = null;
let globalAnnouncementText = null;
let globalAnnouncementTimer = null;
let globalAnnouncementCountdown = 0;
let globalAnnouncementCountdownTimer = null;
let activeAnnouncementId = "";
let queuedAnnouncement = null;
let roomActionOverlay = null;
let roomActionTitle = null;
let roomActionText = null;
let roomActionTimer = null;
let roomActionTimerInterval = null;
let roomActionStartedAt = 0;
let roomActionMode = "";
let roomActionBusy = false;
const ROOM_ACTION_MIN_MS = 2600;

const I18N = {
  ar: {
    pageTitle: "منصة Bedna NL3B",
    langLabel: "اللغة",
    splashLabel: "مرحبًا بك في",
    splashSub: "بوابة سيرفر الألعاب",
    authTitle: "ابدأ رحلتك",
    authDesc: "أنشئ حسابًا جديدًا أو سجّل الدخول إلى حسابك الحالي",
    tabRegister: "تسجيل",
    tabLogin: "دخول",
    usernameLabel: "اسم المستخدم",
    passwordLabel: "كلمة المرور",
    authBtnRegister: "إنشاء حساب",
    authBtnLogin: "دخول",
    lobbyTitle: "اللوبي",
    welcomePrefix: "مرحبًا",
    siteStatsText: "المتواجدون الآن: {online} | داخل الغرف: {inRooms}",
    logoutBtn: "تسجيل الخروج",
    createRoomTitle: "إنشاء غرفة",
    roomNameLabel: "اسم الغرفة",
    roomNamePlaceholder: "مثال: غرفة البطولات",
    createRoomBtn: "إنشاء",
    joinRoomTitle: "الانضمام برمز الدعوة",
    roomCodeLabel: "رمز الغرفة",
    joinRoomBtn: "انضمام",
    publicRoomsTitle: "الغرف المتاحة",
    publicRoomsDesc: "يمكنك طلب الانضمام لأي غرفة، ويجب أن يوافق القائد.",
    ownerRightsTitle: "حقوق مالك السيرفر",
    ownerRightsText: "تم إنشاء هذا الموقع بواسطة",
    ownerRightsImageHint: "الصورة الرسمية لمالك السيرفر",
    ownerRightsName: "Q.",
    ownerRightsCopy: "جميع الحقوق محفوظة ©",
    refreshRoomsBtn: "تحديث",
    roomsEmpty: "لا توجد غرف متاحة الآن.",
    roomCodeText: "الرمز",
    roomHostText: "القائد",
    roomMembersText: "الأعضاء",
    openRoomBtn: "فتح الغرفة",
    requestJoinBtn: "طلب انضمام",
    rejoinDirectBtn: "دخول مباشر",
    supervisorJoinBtn: "انضمام مشرف",
    supervisorJoinBadge: "دخول مشرف",
    requestPendingBtn: "الطلب قيد الانتظار",
    joinAlreadyMember: "أنت عضو",
    joinHostBadge: "أنت القائد",
    approvedRoomBadge: "دخول بلا دعوة",
    supervisorMenuToggle: "قائمة المشرف",
    supervisorOpenBtn: "لوحة المشرف",
    supervisorSidebarTitle: "لوحة المشرف",
    supervisorTabAnnouncement: "الرسالة العامة",
    supervisorTabUsers: "كل الحسابات",
    supervisorTabAppeals: "طلبات رفع الحظر",
    supervisorMenuClose: "إغلاق",
    supervisorAppealsTitle: "طلبات رفع الحظر",
    supervisorAppealsDesc: "طلبات المستخدمين لفك الحظر من الموقع.",
    supervisorAppealsRefreshBtn: "تحديث الطلبات",
    supervisorAppealsEmpty: "لا توجد طلبات رفع حظر.",
    supervisorAppealBy: "المستخدم",
    supervisorAppealReason: "السبب",
    supervisorAppealStatus: "الحالة",
    supervisorAppealApprove: "قبول الطلب",
    supervisorAppealReject: "رفض الطلب",
    supervisorAppealApproved: "تم قبول الطلب.",
    supervisorAppealRejected: "تم رفض الطلب.",
    supervisorUsersTitle: "كل الحسابات",
    supervisorUsersDesc: "قائمة كل الحسابات المسجلة (المتصلون أولاً).",
    supervisorUsersRefreshBtn: "تحديث الحسابات",
    supervisorAnnouncementTitle: "رسالة عامة",
    supervisorAnnouncementDesc: "تظهر هذه الرسالة إجباريًا لكل المستخدمين لمدة 10 ثوانٍ.",
    supervisorAnnouncementPlaceholder: "اكتب الرسالة العامة هنا...",
    supervisorAnnouncementSendBtn: "إرسال الرسالة",
    supervisorAnnouncementNeedText: "اكتب نص الرسالة العامة أولًا.",
    supervisorAnnouncementDone: "تم إرسال الرسالة العامة.",
    supervisorUsersEmpty: "لا توجد حسابات.",
    supervisorUserOnline: "متصل الآن",
    supervisorUserOffline: "غير متصل",
    supervisorUserCreated: "تاريخ الإنشاء",
    supervisorUserBanned: "محظور",
    supervisorUserNotBanned: "غير محظور",
    supervisorBanBtn: "حظر",
    supervisorUnbanBtn: "فك الحظر",
    supervisorDeleteBtn: "حذف نهائي",
    supervisorBanReasonPrompt: "اكتب سبب الحظر للحساب",
    supervisorBanReasonTooShort: "سبب الحظر يجب أن يكون 3 أحرف على الأقل.",
    supervisorBanDone: "تم حظر الحساب.",
    supervisorUnbanDone: "تم فك حظر الحساب.",
    supervisorDeleteConfirm: "تأكيد الحذف النهائي للحساب {user}؟ سيتم حذف كل بياناته.",
    supervisorDeleteDone: "تم حذف الحساب نهائيًا.",
    pendingRequestsTitle: "طلبات الانضمام",
    pendingEmpty: "لا توجد طلبات حالياً.",
    approveBtn: "قبول",
    rejectBtn: "رفض",
    toastRequestFailed: "فشل الطلب.",
    toastInputShort: "اسم المستخدم 3 أحرف على الأقل وكلمة المرور 4 أحرف على الأقل.",
    toastRegisterOk: "تم إنشاء الحساب بنجاح.",
    toastLoginOk: "تم تسجيل الدخول بنجاح.",
    toastLogoutOk: "تم تسجيل الخروج.",
    toastInvalidCode: "أدخل رمز غرفة صحيح.",
    toastJoinRequestSent: "تم إرسال طلب الانضمام.",
    toastJoinApproved: "تم قبول اللاعب {user}.",
    toastJoinRejected: "تم رفض طلب اللاعب {user}.",
    bannedTitle: "الحساب محظور",
    bannedDesc: "تم حظر هذا الحساب من الموقع. يمكنك إرسال طلب للمشرف لرفع الحظر.",
    bannedReasonText: "سبب الحظر: {reason}",
    bannedDateText: "تاريخ الحظر: {date}",
    appealReasonLabel: "سبب طلب رفع الحظر",
    appealPlaceholder: "اكتب سببك بشكل واضح...",
    appealSubmitBtn: "إرسال الطلب",
    bannedLogoutBtn: "تسجيل الخروج",
    appealSent: "تم إرسال طلب رفع الحظر للمشرف.",
    appealNeedReason: "اكتب سببًا لا يقل عن 8 أحرف.",
    banTransitionTitle: "تم حظر حسابك",
    banTransitionSub: "جارٍ نقلك إلى صفحة الحظر...",
    unbanTransitionTitle: "تم فك الحظر",
    unbanTransitionSubLobby: "يمكنك المتابعة الآن. جارٍ إعادتك إلى اللوبي...",
    unbanTransitionSubAuth: "يمكنك المتابعة الآن. جارٍ إعادتك إلى صفحة الدخول...",
    announcementModalTitle: "رسالة عامة من المشرف",
    announcementModalTimer: "ستختفي خلال {seconds} ثوانٍ",
    creatingRoomTitle: "جاري إنشاء الغرفة",
    creatingRoomText: "يتم تجهيز الغرفة وربط الإعدادات...",
    enteringRoomTitle: "جاري دخول الغرفة",
    enteringRoomText: "يتم التحقق من الصلاحيات وتحميل بيانات الغرفة...",
    actionDuration: "المدة: {seconds} ث",
    soundOn: "الصوت: تشغيل",
    soundOff: "الصوت: إيقاف"
  },
  en: {
    pageTitle: "Bedna NL3B Platform",
    langLabel: "Language",
    splashLabel: "WELCOME TO",
    splashSub: "Game Server Portal",
    authTitle: "Start Your Journey",
    authDesc: "Create a new account or sign in to your existing account",
    tabRegister: "Register",
    tabLogin: "Login",
    usernameLabel: "Username",
    passwordLabel: "Password",
    authBtnRegister: "Create Account",
    authBtnLogin: "Login",
    lobbyTitle: "Lobby",
    welcomePrefix: "Welcome",
    siteStatsText: "Online now: {online} | Inside rooms: {inRooms}",
    logoutBtn: "Logout",
    createRoomTitle: "Create Room",
    roomNameLabel: "Room Name",
    roomNamePlaceholder: "Example: Tournament Room",
    createRoomBtn: "Create",
    joinRoomTitle: "Join by Invite Code",
    roomCodeLabel: "Room Code",
    joinRoomBtn: "Join",
    publicRoomsTitle: "Available Rooms",
    publicRoomsDesc: "You can request to join any room, and the leader must approve.",
    ownerRightsTitle: "Server Owner Rights",
    ownerRightsText: "This website was created by",
    ownerRightsImageHint: "Official server owner image",
    ownerRightsName: "Q.",
    ownerRightsCopy: "All rights reserved ©",
    refreshRoomsBtn: "Refresh",
    roomsEmpty: "No rooms created yet.",
    roomCodeText: "Code",
    roomHostText: "Leader",
    roomMembersText: "Members",
    openRoomBtn: "Open Room",
    requestJoinBtn: "Request Join",
    rejoinDirectBtn: "Direct Join",
    supervisorJoinBtn: "Supervisor Join",
    supervisorJoinBadge: "Supervisor Access",
    requestPendingBtn: "Request Pending",
    joinAlreadyMember: "You are a member",
    joinHostBadge: "You are the leader",
    approvedRoomBadge: "No invite needed",
    supervisorMenuToggle: "Supervisor Menu",
    supervisorOpenBtn: "Supervisor Panel",
    supervisorSidebarTitle: "Supervisor Panel",
    supervisorTabAnnouncement: "Global Message",
    supervisorTabUsers: "All Accounts",
    supervisorTabAppeals: "Unban Requests",
    supervisorMenuClose: "Close",
    supervisorAppealsTitle: "Unban Requests",
    supervisorAppealsDesc: "Users' requests to remove site bans.",
    supervisorAppealsRefreshBtn: "Refresh Requests",
    supervisorAppealsEmpty: "No unban requests.",
    supervisorAppealBy: "User",
    supervisorAppealReason: "Reason",
    supervisorAppealStatus: "Status",
    supervisorAppealApprove: "Approve",
    supervisorAppealReject: "Reject",
    supervisorAppealApproved: "Request approved.",
    supervisorAppealRejected: "Request rejected.",
    supervisorUsersTitle: "All Accounts",
    supervisorUsersDesc: "All registered accounts (online first).",
    supervisorUsersRefreshBtn: "Refresh Accounts",
    supervisorAnnouncementTitle: "Global Message",
    supervisorAnnouncementDesc: "This message is forced for all users for 10 seconds.",
    supervisorAnnouncementPlaceholder: "Write the global message here...",
    supervisorAnnouncementSendBtn: "Send Message",
    supervisorAnnouncementNeedText: "Please write the announcement text first.",
    supervisorAnnouncementDone: "Global message sent.",
    supervisorUsersEmpty: "No accounts found.",
    supervisorUserOnline: "Online now",
    supervisorUserOffline: "Offline",
    supervisorUserCreated: "Created",
    supervisorUserBanned: "Banned",
    supervisorUserNotBanned: "Not banned",
    supervisorBanBtn: "Ban",
    supervisorUnbanBtn: "Unban",
    supervisorDeleteBtn: "Delete Forever",
    supervisorBanReasonPrompt: "Enter ban reason for this account",
    supervisorBanReasonTooShort: "Ban reason must be at least 3 characters.",
    supervisorBanDone: "Account banned.",
    supervisorUnbanDone: "Account unbanned.",
    supervisorDeleteConfirm: "Confirm permanent deletion of {user}? All account data will be removed.",
    supervisorDeleteDone: "Account was permanently deleted.",
    pendingRequestsTitle: "Join Requests",
    pendingEmpty: "No pending requests.",
    approveBtn: "Approve",
    rejectBtn: "Reject",
    toastRequestFailed: "Request failed.",
    toastInputShort: "Username must be at least 3 chars and password at least 4 chars.",
    toastRegisterOk: "Account created successfully.",
    toastLoginOk: "Logged in successfully.",
    toastLogoutOk: "Logged out successfully.",
    toastInvalidCode: "Enter a valid room code.",
    toastJoinRequestSent: "Join request sent.",
    toastJoinApproved: "{user} was approved.",
    toastJoinRejected: "{user} was rejected.",
    bannedTitle: "Account Banned",
    bannedDesc: "This account is banned from the site. You can send an unban request to the supervisor.",
    bannedReasonText: "Ban reason: {reason}",
    bannedDateText: "Ban date: {date}",
    appealReasonLabel: "Reason for unban request",
    appealPlaceholder: "Write your reason clearly...",
    appealSubmitBtn: "Send Request",
    bannedLogoutBtn: "Logout",
    appealSent: "Your unban request was sent to the supervisor.",
    appealNeedReason: "Please write at least 8 characters.",
    banTransitionTitle: "Your account was banned",
    banTransitionSub: "Moving you to the ban page...",
    unbanTransitionTitle: "Ban removed",
    unbanTransitionSubLobby: "You can continue now. Returning you to the lobby...",
    unbanTransitionSubAuth: "You can continue now. Returning you to the login page...",
    announcementModalTitle: "Global Message From Supervisor",
    announcementModalTimer: "Closes in {seconds}s",
    creatingRoomTitle: "Creating Room",
    creatingRoomText: "Preparing room settings and connection...",
    enteringRoomTitle: "Entering Room",
    enteringRoomText: "Checking access and loading room data...",
    actionDuration: "Duration: {seconds}s",
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

function showToast(text, type = "error") {
  if (!text) {
    return;
  }
  sfx(type === "success" ? "success" : "error");
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  const toastText = document.createElement("span");
  toastText.className = "toast-text";
  toastText.textContent = text;
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "toast-close";
  closeBtn.textContent = "✕";
  closeBtn.setAttribute("aria-label", t("supervisorMenuClose"));
  closeBtn.title = t("supervisorMenuClose");
  toast.appendChild(toastText);
  toast.appendChild(closeBtn);
  toastContainer.appendChild(toast);
  let dismissed = false;
  const dismiss = () => {
    if (dismissed) {
      return;
    }
    dismissed = true;
    toast.classList.add("hide");
    setTimeout(() => toast.remove(), 300);
  };
  closeBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    dismiss();
  });
  setTimeout(dismiss, 2600);
}

function ensureRoomActionOverlay() {
  if (roomActionOverlay) {
    return roomActionOverlay;
  }
  roomActionOverlay = document.createElement("div");
  roomActionOverlay.className = "modal-overlay operation-modal-overlay hidden";
  roomActionOverlay.innerHTML = `
    <div class="modal-card operation-modal-card">
      <div class="operation-loader" aria-hidden="true"></div>
      <h3 id="roomActionTitle"></h3>
      <p id="roomActionText" class="muted"></p>
      <p id="roomActionTimer" class="muted"></p>
    </div>
  `;
  document.body.appendChild(roomActionOverlay);
  roomActionTitle = roomActionOverlay.querySelector("#roomActionTitle");
  roomActionText = roomActionOverlay.querySelector("#roomActionText");
  roomActionTimer = roomActionOverlay.querySelector("#roomActionTimer");
  return roomActionOverlay;
}

function updateRoomActionOverlayText() {
  if (!roomActionTitle || !roomActionText || !roomActionTimer || !roomActionMode) {
    return;
  }
  const isCreate = roomActionMode === "create";
  roomActionTitle.textContent = isCreate ? t("creatingRoomTitle") : t("enteringRoomTitle");
  roomActionText.textContent = isCreate ? t("creatingRoomText") : t("enteringRoomText");
  const seconds = Math.max(0, Math.floor((Date.now() - roomActionStartedAt) / 1000));
  roomActionTimer.textContent = fmt(t("actionDuration"), { seconds });
}

function closeRoomActionOverlay() {
  if (roomActionTimerInterval) {
    clearInterval(roomActionTimerInterval);
    roomActionTimerInterval = null;
  }
  if (roomActionOverlay) {
    roomActionOverlay.classList.add("hidden");
  }
  roomActionMode = "";
  roomActionStartedAt = 0;
}

function openRoomActionOverlay(mode) {
  roomActionMode = mode;
  roomActionStartedAt = Date.now();
  const overlay = ensureRoomActionOverlay();
  updateRoomActionOverlayText();
  overlay.classList.remove("hidden");
  if (roomActionTimerInterval) {
    clearInterval(roomActionTimerInterval);
  }
  roomActionTimerInterval = setInterval(updateRoomActionOverlayText, 1000);
}

function redirectToRoom(code) {
  if (!code) {
    return;
  }
  sfx("join");
  window.location.href = `/room.html?code=${encodeURIComponent(code)}`;
}

async function runRoomActionWithOverlay(mode, task) {
  if (roomActionBusy) {
    return null;
  }
  roomActionBusy = true;
  const started = Date.now();
  openRoomActionOverlay(mode);
  try {
    const result = await task();
    const remaining = ROOM_ACTION_MIN_MS - (Date.now() - started);
    if (remaining > 0) {
      await delay(remaining);
    }
    return result;
  } finally {
    closeRoomActionOverlay();
    roomActionBusy = false;
  }
}

async function enterRoomWithOverlay(code) {
  if (!code) {
    return;
  }
  const done = await runRoomActionWithOverlay("enter", async () => true);
  if (!done) {
    return;
  }
  redirectToRoom(code);
}

function openActionDialog(options = {}) {
  const {
    title = "",
    message = "",
    input = false,
    inputPlaceholder = "",
    confirmText = "",
    cancelText = "",
    confirmClass = "btn"
  } = options;

  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "modal-overlay action-dialog-overlay";

    const card = document.createElement("div");
    card.className = "modal-card action-dialog-card";

    const closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "modal-close-x";
    closeBtn.textContent = "✕";
    closeBtn.setAttribute("aria-label", cancelText || t("supervisorMenuClose"));
    closeBtn.title = cancelText || t("supervisorMenuClose");

    const titleEl = document.createElement("h3");
    titleEl.textContent = title;

    const messageEl = document.createElement("p");
    messageEl.className = "muted action-dialog-message";
    messageEl.textContent = message;
    if (!message) {
      messageEl.classList.add("hidden");
    }

    const inputEl = document.createElement("textarea");
    inputEl.className = "action-dialog-input";
    inputEl.rows = 4;
    inputEl.maxLength = 500;
    inputEl.placeholder = inputPlaceholder;
    if (!input) {
      inputEl.classList.add("hidden");
    }

    const actions = document.createElement("div");
    actions.className = "room-actions";

    const cancelBtn = document.createElement("button");
    cancelBtn.type = "button";
    cancelBtn.className = "btn btn-ghost";
    cancelBtn.textContent = cancelText || t("supervisorMenuClose");

    const confirmBtn = document.createElement("button");
    confirmBtn.type = "button";
    confirmBtn.className = confirmClass;
    confirmBtn.textContent = confirmText || t("approveBtn");

    actions.appendChild(cancelBtn);
    actions.appendChild(confirmBtn);

    card.appendChild(closeBtn);
    card.appendChild(titleEl);
    card.appendChild(messageEl);
    card.appendChild(inputEl);
    card.appendChild(actions);
    overlay.appendChild(card);
    document.body.appendChild(overlay);

    let done = false;
    const finish = (value) => {
      if (done) {
        return;
      }
      done = true;
      document.removeEventListener("keydown", onKeyDown);
      overlay.remove();
      resolve(value);
    };

    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        finish(null);
      }
    };

    closeBtn.addEventListener("click", () => finish(null));
    cancelBtn.addEventListener("click", () => finish(null));
    confirmBtn.addEventListener("click", () => {
      finish(input ? String(inputEl.value || "").trim() : true);
    });
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) {
        finish(null);
      }
    });
    document.addEventListener("keydown", onKeyDown);

    if (input) {
      requestAnimationFrame(() => {
        inputEl.focus();
      });
    } else {
      requestAnimationFrame(() => {
        confirmBtn.focus();
      });
    }
    sfx("modal");
  });
}

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setSession(token, username) {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, username);
}

function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
  localStorage.removeItem(PENDING_JOIN_KEY);
  hideGlobalAnnouncement();
  closeRoomActionOverlay();
  queuedAnnouncement = null;
}

async function logoutSession() {
  const token = getToken();
  if (token) {
    try {
      await fetch("/api/logout", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Lang": getLang(),
          Authorization: `Bearer ${token}`
        },
        body: "{}"
      });
    } catch (_) {
      // Ignore network failures and continue local sign-out.
    }
  }
  clearSession();
}

function ensureGlobalAnnouncementOverlay() {
  if (globalAnnouncementOverlay) {
    return globalAnnouncementOverlay;
  }
  globalAnnouncementOverlay = document.createElement("div");
  globalAnnouncementOverlay.className = "modal-overlay forced-announcement-overlay hidden";
  globalAnnouncementOverlay.innerHTML = `
    <div class="modal-card forced-announcement-card">
      <h3 id="globalAnnouncementTitle"></h3>
      <p id="globalAnnouncementText" class="forced-announcement-text"></p>
      <p id="globalAnnouncementTimer" class="muted"></p>
    </div>
  `;
  document.body.appendChild(globalAnnouncementOverlay);
  globalAnnouncementTitle = globalAnnouncementOverlay.querySelector("#globalAnnouncementTitle");
  globalAnnouncementText = globalAnnouncementOverlay.querySelector("#globalAnnouncementText");
  globalAnnouncementTimer = globalAnnouncementOverlay.querySelector("#globalAnnouncementTimer");
  return globalAnnouncementOverlay;
}

function clearGlobalAnnouncementTimer() {
  if (globalAnnouncementCountdownTimer) {
    clearInterval(globalAnnouncementCountdownTimer);
    globalAnnouncementCountdownTimer = null;
  }
}

function updateGlobalAnnouncementTimerText() {
  if (!globalAnnouncementTimer) {
    return;
  }
  globalAnnouncementTimer.textContent = fmt(t("announcementModalTimer"), {
    seconds: globalAnnouncementCountdown
  });
}

function hideGlobalAnnouncement() {
  clearGlobalAnnouncementTimer();
  if (globalAnnouncementOverlay) {
    globalAnnouncementOverlay.classList.add("hidden");
  }
  document.body.classList.remove("announcement-lock");
  activeAnnouncementId = "";
}

function processGlobalAnnouncement(payload) {
  const id = String(payload?.id || "").trim();
  const text = String(payload?.text || "").trim();
  if (!id || !text) {
    return;
  }
  const seenId = String(localStorage.getItem(ANNOUNCEMENT_SEEN_KEY) || "");
  if (seenId === id || activeAnnouncementId === id) {
    return;
  }
  if (activeAnnouncementId && activeAnnouncementId !== id) {
    queuedAnnouncement = { id, text };
    return;
  }

  const overlay = ensureGlobalAnnouncementOverlay();
  localStorage.setItem(ANNOUNCEMENT_SEEN_KEY, id);
  activeAnnouncementId = id;
  globalAnnouncementTitle.textContent = t("announcementModalTitle");
  globalAnnouncementText.textContent = text;
  globalAnnouncementCountdown = 10;
  updateGlobalAnnouncementTimerText();
  overlay.classList.remove("hidden");
  document.body.classList.add("announcement-lock");
  sfx("notify");
  clearGlobalAnnouncementTimer();
  globalAnnouncementCountdownTimer = setInterval(() => {
    globalAnnouncementCountdown -= 1;
    if (globalAnnouncementCountdown <= 0) {
      hideGlobalAnnouncement();
      if (queuedAnnouncement) {
        const next = queuedAnnouncement;
        queuedAnnouncement = null;
        processGlobalAnnouncement(next);
      }
      return;
    }
    updateGlobalAnnouncementTimerText();
  }, 1000);
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
  if (data && data.announcement) {
    processGlobalAnnouncement(data.announcement);
  }
  if (!response.ok) {
    const error = new Error(data.error || t("toastRequestFailed"));
    error.status = response.status;
    error.code = data.code || "";
    error.data = data;
    throw error;
  }
  return data;
}

function stopLobbyPolling() {
  if (lobbyPollTimer) {
    clearInterval(lobbyPollTimer);
    lobbyPollTimer = null;
  }
}

function stopBanStatusPolling() {
  if (banStatusPollTimer) {
    clearInterval(banStatusPollTimer);
    banStatusPollTimer = null;
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureAccountStateOverlay() {
  if (accountStateOverlay) {
    return accountStateOverlay;
  }
  accountStateOverlay = document.createElement("div");
  accountStateOverlay.className = "account-state-overlay hidden";
  accountStateOverlay.innerHTML = `
    <div class="account-state-card">
      <div class="account-state-icon" aria-hidden="true"></div>
      <h3 id="accountStateTitle"></h3>
      <p id="accountStateSubtitle"></p>
    </div>
  `;
  document.body.appendChild(accountStateOverlay);
  return accountStateOverlay;
}

async function playAccountStateTransition(type, title, subtitle) {
  if (accountTransitionBusy) {
    return;
  }
  accountTransitionBusy = true;
  const overlay = ensureAccountStateOverlay();
  const titleEl = overlay.querySelector("#accountStateTitle");
  const subtitleEl = overlay.querySelector("#accountStateSubtitle");
  titleEl.textContent = title || "";
  subtitleEl.textContent = subtitle || "";
  overlay.classList.remove("hidden", "is-ban", "is-unban", "is-leaving");
  overlay.classList.add(type === "unban" ? "is-unban" : "is-ban");
  requestAnimationFrame(() => {
    overlay.classList.add("is-visible");
  });
  await delay(1150);
  overlay.classList.add("is-leaving");
  await delay(360);
  overlay.classList.remove("is-visible", "is-ban", "is-unban", "is-leaving");
  overlay.classList.add("hidden");
  accountTransitionBusy = false;
}

function setSupervisorMenuOpen(open) {
  if (!supervisorSidebar || !supervisorSidebarOverlay || !supervisorMenuToggleBtn) {
    return;
  }
  const canOpen = isSupervisor && Boolean(getToken());
  const nextOpen = Boolean(open && canOpen);
  isSupervisorMenuOpen = nextOpen;
  supervisorSidebar.classList.toggle("hidden", !nextOpen);
  supervisorSidebarOverlay.classList.toggle("hidden", !nextOpen);
  supervisorSidebar.classList.toggle("is-open", nextOpen);
  supervisorSidebarOverlay.classList.toggle("is-open", nextOpen);
  supervisorSidebar.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  supervisorSidebarOverlay.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  supervisorMenuToggleBtn.setAttribute("aria-expanded", nextOpen ? "true" : "false");
  document.body.classList.toggle("supervisor-menu-open", nextOpen);
  if (nextOpen) {
    setActiveSupervisorTab(activeSupervisorTab, false);
  }
}

function setActiveSupervisorTab(tabName, scrollIntoView = true) {
  const allowed = ["announcement", "users", "appeals"];
  const nextTab = allowed.includes(tabName) ? tabName : "announcement";
  activeSupervisorTab = nextTab;
  if (!supervisorTabAnnouncement || !supervisorTabUsers || !supervisorTabAppeals) {
    return;
  }
  supervisorTabAnnouncement.classList.toggle("active", nextTab === "announcement");
  supervisorTabUsers.classList.toggle("active", nextTab === "users");
  supervisorTabAppeals.classList.toggle("active", nextTab === "appeals");
  if (!scrollIntoView) {
    return;
  }
  const targetSection = nextTab === "announcement"
    ? supervisorAnnouncementSection
    : nextTab === "users"
      ? supervisorUsersSection
      : supervisorAppealsSection;
  focusSupervisorSection(targetSection);
}

function focusSupervisorSection(sectionEl) {
  if (!sectionEl) {
    return;
  }
  sectionEl.scrollIntoView({ behavior: "smooth", block: "start" });
  sectionEl.classList.add("section-focus-flash");
  setTimeout(() => {
    sectionEl.classList.remove("section-focus-flash");
  }, 900);
}

function syncSupervisorControls() {
  const showSupervisorControls = isSupervisor && Boolean(getToken());
  supervisorMenuToggleBtn.classList.toggle("hidden", !showSupervisorControls);
  supervisorAnnouncementSection.classList.toggle("hidden", !showSupervisorControls);
  supervisorAppealsSection.classList.toggle("hidden", !showSupervisorControls);
  supervisorUsersSection.classList.toggle("hidden", !showSupervisorControls);
  setSupervisorMenuOpen(false);
}

function showAuth() {
  stopLobbyPolling();
  stopBanStatusPolling();
  hideGlobalAnnouncement();
  queuedAnnouncement = null;
  currentBanInfo = null;
  bannedUsername = "";
  isSupervisor = false;
  cachedAppeals = [];
  cachedSupervisorUsers = [];
  authSection.classList.remove("hidden");
  lobbySection.classList.add("hidden");
  bannedSection.classList.add("hidden");
  syncSupervisorControls();
}

function showLobby() {
  stopBanStatusPolling();
  authSection.classList.add("hidden");
  lobbySection.classList.remove("hidden");
  bannedSection.classList.add("hidden");
  currentBanInfo = null;
  syncSupervisorControls();
  setSupervisorMenuOpen(false);
  currentUser.textContent = localStorage.getItem(USER_KEY) || "";
  refreshPublicRooms();
  stopLobbyPolling();
  lobbyPollTimer = setInterval(refreshPublicRooms, 2500);
}

function showBanned(ban, username = "") {
  stopLobbyPolling();
  stopBanStatusPolling();
  authSection.classList.add("hidden");
  lobbySection.classList.add("hidden");
  bannedSection.classList.remove("hidden");
  bannedSection.classList.remove("ban-in");
  requestAnimationFrame(() => {
    bannedSection.classList.add("ban-in");
  });
  syncSupervisorControls();
  bannedUsername = String(username || localStorage.getItem(USER_KEY) || "").toLowerCase();
  currentBanInfo = ban || null;
  const reason = ban?.reason || "-";
  const date = formatDate(ban?.bannedAt);
  bannedReasonText.textContent = fmt(t("bannedReasonText"), { reason });
  bannedDateText.textContent = fmt(t("bannedDateText"), { date });
  startBanStatusWatcher();
}

async function transitionToBanned(ban, username = "") {
  await playAccountStateTransition("ban", t("banTransitionTitle"), t("banTransitionSub"));
  showBanned(ban, username);
}

async function checkBanStatusOnce() {
  if (!bannedUsername || accountTransitionBusy) {
    return;
  }
  try {
    const status = await api(`/api/ban-appeals/${encodeURIComponent(bannedUsername)}`);
    if (status?.isBanned) {
      currentBanInfo = status.ban || currentBanInfo;
      bannedReasonText.textContent = fmt(t("bannedReasonText"), { reason: currentBanInfo?.reason || "-" });
      bannedDateText.textContent = fmt(t("bannedDateText"), { date: formatDate(currentBanInfo?.bannedAt) });
      return;
    }
    const token = getToken();
    if (!token) {
      stopBanStatusPolling();
      await playAccountStateTransition("unban", t("unbanTransitionTitle"), t("unbanTransitionSubAuth"));
      showAuth();
      return;
    }

    let me;
    try {
      me = await api("/api/me");
    } catch (error) {
      if (error?.status === 401) {
        stopBanStatusPolling();
        await playAccountStateTransition("unban", t("unbanTransitionTitle"), t("unbanTransitionSubAuth"));
        showAuth();
      }
      return;
    }

    if (me?.banned) {
      return;
    }

    stopBanStatusPolling();
    localStorage.setItem(USER_KEY, me.username);
    isSupervisor = Boolean(me.isSupervisor);
    await playAccountStateTransition("unban", t("unbanTransitionTitle"), t("unbanTransitionSubLobby"));
    showLobby();
  } catch (_) {
    // Ignore transient network/API failures; next tick will retry.
  }
}

function startBanStatusWatcher() {
  stopBanStatusPolling();
  if (!bannedUsername) {
    return;
  }
  checkBanStatusOnce();
  banStatusPollTimer = setInterval(checkBanStatusOnce, 3200);
}

function renderSiteStats(stats = {}) {
  cachedSiteStats = {
    onlineUsers: Number(stats.onlineUsers || 0),
    usersInRooms: Number(stats.usersInRooms || 0)
  };
  const online = cachedSiteStats.onlineUsers;
  const inRooms = cachedSiteStats.usersInRooms;
  siteStatsText.textContent = fmt(t("siteStatsText"), { online, inRooms });
}

function renderSupervisorAppeals(requests) {
  cachedAppeals = requests.slice();
  supervisorAppealsList.innerHTML = "";
  if (!isSupervisor) {
    return;
  }
  if (requests.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("supervisorAppealsEmpty");
    supervisorAppealsList.appendChild(empty);
    return;
  }
  requests.forEach((item) => {
    const card = document.createElement("article");
    card.className = "room-card";
    const title = document.createElement("h4");
    title.textContent = `${t("supervisorAppealBy")}: ${item.username}`;
    card.appendChild(title);
    card.appendChild(roomCardInfo(t("supervisorAppealReason"), item.reason || "-"));
    card.appendChild(roomCardInfo(t("supervisorAppealStatus"), item.status || "pending"));
    if (item.status === "pending") {
      const actions = document.createElement("div");
      actions.className = "room-actions";
      const approveBtn = document.createElement("button");
      approveBtn.type = "button";
      approveBtn.className = "btn btn-approve";
      approveBtn.textContent = t("supervisorAppealApprove");
      approveBtn.addEventListener("click", () => {
        sfx("click");
        decideAppeal(item.username, "approve");
      });
      actions.appendChild(approveBtn);
      const rejectBtn = document.createElement("button");
      rejectBtn.type = "button";
      rejectBtn.className = "btn btn-reject";
      rejectBtn.textContent = t("supervisorAppealReject");
      rejectBtn.addEventListener("click", () => {
        sfx("click");
        decideAppeal(item.username, "reject");
      });
      actions.appendChild(rejectBtn);
      card.appendChild(actions);
    }
    supervisorAppealsList.appendChild(card);
  });
}

async function refreshSupervisorAppeals() {
  if (!isSupervisor || lobbySection.classList.contains("hidden")) {
    return;
  }
  try {
    const data = await api("/api/admin/ban-appeals");
    renderSupervisorAppeals(data.requests || []);
  } catch (error) {
    showToast(error.message);
  }
}

async function handleSupervisorBanUser(username) {
  const reason = await openActionDialog({
    title: t("supervisorBanBtn"),
    message: t("supervisorBanReasonPrompt"),
    input: true,
    inputPlaceholder: t("supervisorBanReasonPrompt"),
    confirmText: t("supervisorBanBtn"),
    cancelText: t("supervisorMenuClose"),
    confirmClass: "btn btn-reject"
  });
  if (reason === null) {
    return false;
  }
  if (reason.length < 3) {
    if (reason.length > 0) {
      showToast(t("supervisorBanReasonTooShort"));
    }
    return false;
  }
  await api("/api/admin/ban-user", {
    method: "POST",
    body: {
      username,
      reason
    }
  });
  return true;
}

async function handleSupervisorUnbanUser(username) {
  await api("/api/admin/unban-user", {
    method: "POST",
    body: {
      username,
      note: "Unbanned from accounts panel"
    }
  });
  return true;
}

async function handleSupervisorDeleteUser(username) {
  const confirmDelete = await openActionDialog({
    title: t("supervisorDeleteBtn"),
    message: fmt(t("supervisorDeleteConfirm"), { user: username }),
    confirmText: t("supervisorDeleteBtn"),
    cancelText: t("supervisorMenuClose"),
    confirmClass: "btn btn-danger"
  });
  if (!confirmDelete) {
    return false;
  }
  await api("/api/admin/delete-user", {
    method: "POST",
    body: { username }
  });
  showToast(t("supervisorDeleteDone"), "success");
  return true;
}

function renderSupervisorUsers(users) {
  cachedSupervisorUsers = users.slice();
  supervisorUsersList.innerHTML = "";
  if (!isSupervisor) {
    return;
  }
  if (users.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("supervisorUsersEmpty");
    supervisorUsersList.appendChild(empty);
    return;
  }

  users.forEach((item) => {
    const card = document.createElement("article");
    card.className = "admin-user-card";
    const title = document.createElement("h4");
    title.className = "admin-user-title";
    const titleText = document.createElement("span");
    titleText.textContent = `@${item.username}`;
    const onlineDot = document.createElement("span");
    onlineDot.className = `online-dot ${item.isOnline ? "is-online" : ""}`;
    title.appendChild(titleText);
    title.appendChild(onlineDot);
    card.appendChild(title);

    card.appendChild(roomCardInfo(item.displayName || item.username, item.isOnline ? t("supervisorUserOnline") : t("supervisorUserOffline")));
    card.appendChild(roomCardInfo(t("supervisorUserCreated"), formatDate(item.createdAt)));
    card.appendChild(roomCardInfo(t("supervisorAppealStatus"), item.isBanned ? t("supervisorUserBanned") : t("supervisorUserNotBanned")));

    if (!item.isSupervisor) {
      const actions = document.createElement("div");
      actions.className = "room-actions";
      if (item.isBanned) {
        const unbanBtn = document.createElement("button");
        unbanBtn.type = "button";
        unbanBtn.className = "btn btn-approve";
        unbanBtn.textContent = t("supervisorUnbanBtn");
        unbanBtn.addEventListener("click", async () => {
          sfx("click");
          try {
            const changed = await handleSupervisorUnbanUser(item.username);
            if (!changed) {
              return;
            }
            showToast(t("supervisorUnbanDone"), "success");
            await refreshSupervisorUsers();
            await refreshPublicRooms();
          } catch (error) {
            showToast(error.message);
          }
        });
        actions.appendChild(unbanBtn);
      } else {
        const banBtn = document.createElement("button");
        banBtn.type = "button";
        banBtn.className = "btn btn-reject";
        banBtn.textContent = t("supervisorBanBtn");
        banBtn.addEventListener("click", async () => {
          sfx("click");
          try {
            const changed = await handleSupervisorBanUser(item.username);
            if (!changed) {
              return;
            }
            showToast(t("supervisorBanDone"), "success");
            await refreshSupervisorUsers();
            await refreshPublicRooms();
          } catch (error) {
            showToast(error.message);
          }
        });
        actions.appendChild(banBtn);
      }

      const deleteBtn = document.createElement("button");
      deleteBtn.type = "button";
      deleteBtn.className = "btn btn-danger";
      deleteBtn.textContent = t("supervisorDeleteBtn");
      deleteBtn.addEventListener("click", async () => {
        sfx("click");
        try {
          const deleted = await handleSupervisorDeleteUser(item.username);
          if (!deleted) {
            return;
          }
          await refreshSupervisorUsers();
          await refreshPublicRooms();
        } catch (error) {
          showToast(error.message);
        }
      });
      actions.appendChild(deleteBtn);
      card.appendChild(actions);
    }

    supervisorUsersList.appendChild(card);
  });
}

async function refreshSupervisorUsers() {
  if (!isSupervisor || lobbySection.classList.contains("hidden")) {
    return;
  }
  try {
    const data = await api("/api/admin/users");
    renderSupervisorUsers(data.users || []);
  } catch (error) {
    showToast(error.message);
  }
}

async function sendSupervisorAnnouncement() {
  const text = String(supervisorAnnouncementInput.value || "").trim();
  if (!text) {
    showToast(t("supervisorAnnouncementNeedText"));
    return;
  }
  try {
    await api("/api/admin/site-announcement", {
      method: "POST",
      body: { text }
    });
    supervisorAnnouncementInput.value = "";
    showToast(t("supervisorAnnouncementDone"), "success");
  } catch (error) {
    showToast(error.message);
  }
}

async function decideAppeal(username, action) {
  try {
    await api("/api/admin/ban-appeals/decision", {
      method: "POST",
      body: {
        username,
        action,
        note: action === "approve" ? "Approved by supervisor" : "Rejected by supervisor"
      }
    });
    showToast(action === "approve" ? t("supervisorAppealApproved") : t("supervisorAppealRejected"), "success");
    await refreshSupervisorAppeals();
  } catch (error) {
    showToast(error.message);
  }
}

function updateModeUI() {
  tabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.tab === mode);
  });
  authBtn.textContent = mode === "register" ? t("authBtnRegister") : t("authBtnLogin");
}

function roomCardInfo(label, value) {
  const row = document.createElement("p");
  row.className = "muted room-line";
  row.textContent = `${label}: ${value}`;
  return row;
}

function roomMetaItem(label, value) {
  const item = document.createElement("div");
  item.className = "room-meta-item";

  const metaLabel = document.createElement("span");
  metaLabel.className = "room-meta-label";
  metaLabel.textContent = label;

  const metaValue = document.createElement("span");
  metaValue.className = "room-meta-value";
  metaValue.textContent = String(value ?? "-");

  item.appendChild(metaLabel);
  item.appendChild(metaValue);
  return item;
}

async function autoOpenApprovedRoom(rooms) {
  const pendingCode = String(localStorage.getItem(PENDING_JOIN_KEY) || "").trim().toUpperCase();
  if (!pendingCode) {
    return false;
  }
  const room = rooms.find((item) => item.code === pendingCode);
  if (!room) {
    localStorage.removeItem(PENDING_JOIN_KEY);
    return false;
  }
  if (room.isMember) {
    localStorage.removeItem(PENDING_JOIN_KEY);
    await enterRoomWithOverlay(room.code);
    return true;
  }
  if (!room.hasPendingRequest) {
    localStorage.removeItem(PENDING_JOIN_KEY);
  }
  return false;
}

async function handleJoinRequest(code) {
  try {
    const result = await api(`/api/rooms/${encodeURIComponent(code)}/request-join`, { method: "POST" });
    if (result.status === "already_member" && result.room?.code) {
      localStorage.removeItem(PENDING_JOIN_KEY);
      await enterRoomWithOverlay(result.room.code);
      return;
    }
    localStorage.setItem(PENDING_JOIN_KEY, code);
    showToast(t("toastJoinRequestSent"), "success");
    await refreshPublicRooms();
  } catch (error) {
    if (error.code === "ACCOUNT_BANNED") {
      await transitionToBanned(error.data?.ban);
      return;
    }
    showToast(error.message);
  }
}

async function handleSupervisorDirectJoin(code) {
  try {
    const result = await runRoomActionWithOverlay("enter", async () =>
      api("/api/rooms/join", {
        method: "POST",
        body: { code }
      })
    );
    if (result?.room?.code) {
      localStorage.removeItem(PENDING_JOIN_KEY);
      redirectToRoom(result.room.code);
    }
  } catch (error) {
    if (error.code === "ACCOUNT_BANNED") {
      await transitionToBanned(error.data?.ban);
      return;
    }
    showToast(error.message);
  }
}

async function handleRequestDecision(code, username, action) {
  try {
    await api(`/api/rooms/${encodeURIComponent(code)}/requests`, {
      method: "POST",
      body: { username, action }
    });
    if (action === "approve") {
      showToast(fmt(t("toastJoinApproved"), { user: username }), "success");
    } else {
      showToast(fmt(t("toastJoinRejected"), { user: username }), "success");
    }
    await refreshPublicRooms();
  } catch (error) {
    showToast(error.message);
  }
}

function renderPendingRequests(room) {
  const wrap = document.createElement("div");
  wrap.className = "pending-requests";

  const title = document.createElement("h4");
  title.textContent = t("pendingRequestsTitle");
  wrap.appendChild(title);

  if (!room.pendingRequests || room.pendingRequests.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("pendingEmpty");
    wrap.appendChild(empty);
    return wrap;
  }

  room.pendingRequests.forEach((requestUser) => {
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
    approveBtn.addEventListener("click", () => {
      sfx("click");
      handleRequestDecision(room.code, requestUser, "approve");
    });
    actions.appendChild(approveBtn);

    const rejectBtn = document.createElement("button");
    rejectBtn.type = "button";
      rejectBtn.className = "btn btn-reject";
      rejectBtn.textContent = t("rejectBtn");
    rejectBtn.addEventListener("click", () => {
      sfx("click");
      handleRequestDecision(room.code, requestUser, "reject");
    });
    actions.appendChild(rejectBtn);

    item.appendChild(actions);
    wrap.appendChild(item);
  });

  return wrap;
}

function renderPublicRooms(rooms) {
  cachedRooms = rooms.slice();
  publicRoomsList.innerHTML = "";

  if (rooms.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("roomsEmpty");
    publicRoomsList.appendChild(empty);
    return;
  }

  rooms.forEach((room) => {
    const card = document.createElement("article");
    card.className = "room-card room-card-lobby";
    if (!room.isMember && room.isApproved) {
      card.classList.add("approved-room");
    }
    if (room.isHost) {
      card.classList.add("host-room");
    }

    const titleRow = document.createElement("div");
    titleRow.className = "room-title-row";
    const title = document.createElement("h4");
    title.textContent = room.name;
    titleRow.appendChild(title);
    card.appendChild(titleRow);

    const pills = document.createElement("div");
    pills.className = "room-top-pills";

    const codePill = document.createElement("span");
    codePill.className = "room-pill room-code-pill";
    codePill.textContent = `${t("roomCodeText")}: ${room.code}`;
    pills.appendChild(codePill);

    const statusPill = document.createElement("span");
    let statusText = t("requestJoinBtn");
    let statusClass = "is-open";
    if (room.isHost) {
      statusText = t("joinHostBadge");
      statusClass = "is-host";
    } else if (room.isMember) {
      statusText = t("joinAlreadyMember");
      statusClass = "is-member";
    } else if (room.hasPendingRequest) {
      statusText = t("requestPendingBtn");
      statusClass = "is-pending";
    } else if (room.isApproved) {
      statusText = t("approvedRoomBadge");
      statusClass = "is-approved";
    }
    statusPill.className = `room-pill room-status-pill ${statusClass}`;
    statusPill.textContent = statusText;
    pills.appendChild(statusPill);
    card.appendChild(pills);

    const metaGrid = document.createElement("div");
    metaGrid.className = "room-meta-grid";
    metaGrid.appendChild(roomMetaItem(t("roomHostText"), room.host));
    metaGrid.appendChild(roomMetaItem(t("roomMembersText"), room.memberCount));
    metaGrid.appendChild(roomMetaItem(t("supervisorUserCreated"), formatDate(room.createdAt)));
    card.appendChild(metaGrid);

    const actions = document.createElement("div");
    actions.className = "room-actions";

    if (room.isMember) {
      const openBtn = document.createElement("button");
      openBtn.type = "button";
      openBtn.className = "btn";
      openBtn.textContent = room.isHost ? t("joinHostBadge") : t("openRoomBtn");
      openBtn.addEventListener("click", async () => {
        await enterRoomWithOverlay(room.code);
      });
      actions.appendChild(openBtn);
    } else if (room.isApproved && !isSupervisor) {
      const directBtn = document.createElement("button");
      directBtn.type = "button";
      directBtn.className = "btn";
      directBtn.textContent = t("rejoinDirectBtn");
      directBtn.addEventListener("click", async () => {
        await enterRoomWithOverlay(room.code);
      });
      actions.appendChild(directBtn);
    } else if (room.hasPendingRequest) {
      const pendingBtn = document.createElement("button");
      pendingBtn.type = "button";
      pendingBtn.className = "btn btn-ghost";
      pendingBtn.disabled = true;
      pendingBtn.textContent = t("requestPendingBtn");
      actions.appendChild(pendingBtn);
    } else {
      const requestBtn = document.createElement("button");
      requestBtn.type = "button";
      requestBtn.className = "btn";
      requestBtn.textContent = t("requestJoinBtn");
      requestBtn.addEventListener("click", () => {
        sfx("click");
        handleJoinRequest(room.code);
      });
      actions.appendChild(requestBtn);
    }

    if (!room.isMember && isSupervisor) {
      const supervisorJoinBtn = document.createElement("button");
      supervisorJoinBtn.type = "button";
      supervisorJoinBtn.className = "btn btn-ghost";
      supervisorJoinBtn.textContent = t("supervisorJoinBtn");
      supervisorJoinBtn.addEventListener("click", () => {
        sfx("join");
        handleSupervisorDirectJoin(room.code);
      });
      actions.appendChild(supervisorJoinBtn);
    }

    card.appendChild(actions);

    if (room.isHost) {
      card.appendChild(renderPendingRequests(room));
    }

    publicRoomsList.appendChild(card);
  });
}

async function refreshPublicRooms() {
  if (!getToken() || lobbySection.classList.contains("hidden")) {
    return;
  }
  if (roomActionBusy) {
    return;
  }
  try {
    const data = await api("/api/rooms");
    const rooms = data.rooms || [];
    renderSiteStats(data.stats || {});
    renderPublicRooms(rooms);
    if (isSupervisor) {
      refreshSupervisorAppeals();
      refreshSupervisorUsers();
    }
    await autoOpenApprovedRoom(rooms);
  } catch (error) {
    if (error.code === "ACCOUNT_BANNED") {
      sfx("ban");
      await transitionToBanned(error.data?.ban);
      return;
    }
    showToast(error.message);
  }
}

function applyTranslations() {
  document.title = t("pageTitle");
  document.getElementById("langLabel").textContent = t("langLabel");
  renderSoundToggle();
  document.getElementById("splashLabel").textContent = t("splashLabel");
  document.getElementById("splashSub").textContent = t("splashSub");
  document.getElementById("authTitle").textContent = t("authTitle");
  document.getElementById("authDesc").textContent = t("authDesc");
  document.getElementById("tabRegister").textContent = t("tabRegister");
  document.getElementById("tabLogin").textContent = t("tabLogin");
  document.getElementById("usernameLabel").textContent = t("usernameLabel");
  document.getElementById("passwordLabel").textContent = t("passwordLabel");
  document.getElementById("lobbyTitle").textContent = t("lobbyTitle");
  document.getElementById("welcomePrefix").textContent = t("welcomePrefix");
  renderSiteStats(cachedSiteStats);
  document.getElementById("logoutBtn").textContent = t("logoutBtn");
  document.getElementById("createRoomTitle").textContent = t("createRoomTitle");
  document.getElementById("roomNameLabel").textContent = t("roomNameLabel");
  document.getElementById("roomName").placeholder = t("roomNamePlaceholder");
  document.getElementById("createRoomBtn").textContent = t("createRoomBtn");
  document.getElementById("joinRoomTitle").textContent = t("joinRoomTitle");
  document.getElementById("roomCodeLabel").textContent = t("roomCodeLabel");
  document.getElementById("joinRoomBtn").textContent = t("joinRoomBtn");
  document.getElementById("publicRoomsTitle").textContent = t("publicRoomsTitle");
  document.getElementById("publicRoomsDesc").textContent = t("publicRoomsDesc");
  document.getElementById("ownerRightsTitle").textContent = t("ownerRightsTitle");
  document.getElementById("ownerRightsText").textContent = t("ownerRightsText");
  document.getElementById("ownerRightsImageHint").textContent = t("ownerRightsImageHint");
  document.getElementById("ownerRightsName").textContent = t("ownerRightsName");
  document.getElementById("ownerRightsCopy").textContent = t("ownerRightsCopy");
  refreshRoomsBtn.textContent = t("refreshRoomsBtn");
  supervisorMenuToggleBtn.textContent = t("supervisorOpenBtn");
  supervisorMenuToggleBtn.setAttribute("aria-label", t("supervisorMenuToggle"));
  supervisorMenuToggleBtn.title = t("supervisorMenuToggle");
  supervisorSidebarTitle.textContent = t("supervisorSidebarTitle");
  supervisorTabAnnouncement.textContent = t("supervisorTabAnnouncement");
  supervisorTabUsers.textContent = t("supervisorTabUsers");
  supervisorTabAppeals.textContent = t("supervisorTabAppeals");
  supervisorMenuCloseBtn.textContent = "✕";
  supervisorMenuCloseBtn.setAttribute("aria-label", t("supervisorMenuClose"));
  supervisorMenuCloseBtn.title = t("supervisorMenuClose");
  supervisorAnnouncementTitle.textContent = t("supervisorAnnouncementTitle");
  supervisorAnnouncementDesc.textContent = t("supervisorAnnouncementDesc");
  supervisorAnnouncementInput.placeholder = t("supervisorAnnouncementPlaceholder");
  supervisorAnnouncementSendBtn.textContent = t("supervisorAnnouncementSendBtn");
  supervisorAppealsTitle.textContent = t("supervisorAppealsTitle");
  supervisorAppealsDesc.textContent = t("supervisorAppealsDesc");
  supervisorAppealsRefreshBtn.textContent = t("supervisorAppealsRefreshBtn");
  supervisorUsersTitle.textContent = t("supervisorUsersTitle");
  supervisorUsersDesc.textContent = t("supervisorUsersDesc");
  supervisorUsersRefreshBtn.textContent = t("supervisorUsersRefreshBtn");
  bannedTitle.textContent = t("bannedTitle");
  bannedDesc.textContent = t("bannedDesc");
  bannedReasonText.textContent = fmt(t("bannedReasonText"), { reason: currentBanInfo?.reason || "-" });
  bannedDateText.textContent = fmt(t("bannedDateText"), { date: formatDate(currentBanInfo?.bannedAt) });
  appealReasonLabel.textContent = t("appealReasonLabel");
  appealReason.placeholder = t("appealPlaceholder");
  appealSubmitBtn.textContent = t("appealSubmitBtn");
  bannedLogoutBtn.textContent = t("bannedLogoutBtn");
  updateModeUI();
  renderPublicRooms(cachedRooms);
  renderSupervisorAppeals(cachedAppeals);
  renderSupervisorUsers(cachedSupervisorUsers);
  setActiveSupervisorTab(activeSupervisorTab, false);
  if (globalAnnouncementOverlay && !globalAnnouncementOverlay.classList.contains("hidden")) {
    globalAnnouncementTitle.textContent = t("announcementModalTitle");
    updateGlobalAnnouncementTimerText();
  }
  if (roomActionOverlay && !roomActionOverlay.classList.contains("hidden")) {
    updateRoomActionOverlayText();
  }
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

tabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    sfx("click");
    mode = tab.dataset.tab;
    updateModeUI();
  });
});

authForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(authForm);
  const username = String(formData.get("username") || "").trim();
  const password = String(formData.get("password") || "");

  if (username.length < 3 || password.length < 4) {
    showToast(t("toastInputShort"));
    return;
  }

  try {
    const endpoint = mode === "register" ? "/api/register" : "/api/login";
    const result = await api(endpoint, {
      method: "POST",
      body: { username, password }
    });
    setSession(result.token, result.username);
    try {
      const meData = await api("/api/me");
      isSupervisor = Boolean(meData.isSupervisor);
    } catch (_) {
      isSupervisor = false;
    }
    showToast(mode === "register" ? t("toastRegisterOk") : t("toastLoginOk"), "success");
    showLobby();
  } catch (error) {
    if (error.code === "ACCOUNT_BANNED") {
      sfx("ban");
      localStorage.setItem(USER_KEY, username.toLowerCase());
      await transitionToBanned(error.data?.ban, username);
      return;
    }
    showToast(error.message);
  }
});

logoutBtn.addEventListener("click", async () => {
  sfx("leave");
  await logoutSession();
  showAuth();
  showToast(t("toastLogoutOk"), "success");
});

createRoomForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (roomActionBusy) {
    return;
  }
  const formData = new FormData(createRoomForm);
  const roomName = String(formData.get("roomName") || "").trim();

  try {
    const result = await runRoomActionWithOverlay("create", async () =>
      api("/api/rooms/create", {
        method: "POST",
        body: { roomName }
      })
    );
    if (result?.room?.code) {
      redirectToRoom(result.room.code);
    }
  } catch (error) {
    showToast(error.message);
  }
});

joinRoomForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (roomActionBusy) {
    return;
  }
  const formData = new FormData(joinRoomForm);
  const code = String(formData.get("roomCode") || "").trim().toUpperCase();

  if (code.length < 4) {
    showToast(t("toastInvalidCode"));
    return;
  }

  await handleJoinRequest(code);
});

refreshRoomsBtn.addEventListener("click", () => {
  sfx("click");
  refreshPublicRooms();
});

if (supervisorMenuToggleBtn) {
  supervisorMenuToggleBtn.addEventListener("click", () => {
    sfx("click");
    setSupervisorMenuOpen(!isSupervisorMenuOpen);
  });
}

if (supervisorMenuCloseBtn) {
  supervisorMenuCloseBtn.addEventListener("click", () => {
    sfx("click");
    setSupervisorMenuOpen(false);
  });
}

if (supervisorSidebarOverlay) {
  supervisorSidebarOverlay.addEventListener("click", () => {
    setSupervisorMenuOpen(false);
  });
}

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && isSupervisorMenuOpen) {
    setSupervisorMenuOpen(false);
  }
});

if (supervisorTabAnnouncement) {
  supervisorTabAnnouncement.addEventListener("click", () => {
    sfx("click");
    setActiveSupervisorTab("announcement");
  });
}

if (supervisorTabUsers) {
  supervisorTabUsers.addEventListener("click", () => {
    sfx("click");
    setActiveSupervisorTab("users");
  });
}

if (supervisorTabAppeals) {
  supervisorTabAppeals.addEventListener("click", () => {
    sfx("click");
    setActiveSupervisorTab("appeals");
  });
}

supervisorAppealsRefreshBtn.addEventListener("click", () => {
  sfx("click");
  refreshSupervisorAppeals();
});

supervisorUsersRefreshBtn.addEventListener("click", () => {
  sfx("click");
  refreshSupervisorUsers();
});

if (supervisorAnnouncementSendBtn) {
  supervisorAnnouncementSendBtn.addEventListener("click", () => {
    sfx("click");
    sendSupervisorAnnouncement();
  });
}

appealForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const reason = String(appealReason.value || "").trim();
  if (reason.length < 8) {
    showToast(t("appealNeedReason"));
    return;
  }
  if (!bannedUsername) {
    showToast(t("toastRequestFailed"));
    return;
  }
  try {
    await api("/api/ban-appeals", {
      method: "POST",
      body: {
        username: bannedUsername,
        reason
      }
    });
    showToast(t("appealSent"), "success");
    appealReason.value = "";
  } catch (error) {
    showToast(error.message);
  }
});

bannedLogoutBtn.addEventListener("click", async () => {
  sfx("leave");
  await logoutSession();
  bannedUsername = "";
  showAuth();
});

async function boot() {
  setTimeout(() => {
    splash.classList.add("hidden");
    app.classList.remove("hidden");
  }, 2100);

  const token = getToken();
  if (!token) {
    showAuth();
    return;
  }

  try {
    const result = await api("/api/me");
    localStorage.setItem(USER_KEY, result.username);
    isSupervisor = Boolean(result.isSupervisor);
    if (result.banned) {
      sfx("ban");
      await transitionToBanned(result.ban, result.username);
      return;
    }
    showLobby();
    const urlData = new URLSearchParams(window.location.search);
    const msg = urlData.get("msg");
    if (msg) {
      showToast(msg, "success");
    }
  } catch (error) {
    clearSession();
    showAuth();
  }
}

setLang(getLang());
boot();


