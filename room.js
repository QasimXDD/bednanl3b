const TOKEN_KEY = "bedna_token";
const USER_KEY = "bedna_user";
const LANG_KEY = "bedna_lang";
const ANNOUNCEMENT_SEEN_KEY = "bedna_seen_announcement_id";

const roomName = document.getElementById("roomName");
const roomCode = document.getElementById("roomCode");
const roomLeader = document.getElementById("roomLeader");
const meName = document.getElementById("meName");
const playersList = document.getElementById("playersList");
const roomMembersCountText = document.getElementById("roomMembersCountText");
const pendingRequestsBox = document.getElementById("pendingRequestsBox");
const pendingRequestsTitle = document.getElementById("pendingRequestsTitle");
const pendingRequestsList = document.getElementById("pendingRequestsList");
const playersDrawer = document.getElementById("playersDrawer");
const playersDrawerToggle = document.getElementById("playersDrawerToggle");
const playersDrawerToggleText = document.getElementById("playersDrawerToggleText");
const playersDrawerCountBadge = document.getElementById("playersDrawerCountBadge");
const playersDrawerClose = document.getElementById("playersDrawerClose");
const playersDrawerOverlay = document.getElementById("playersDrawerOverlay");
const backToLobbyLink = document.getElementById("backToLobby");
const joinRequestModal = document.getElementById("joinRequestModal");
const joinModalTitle = document.getElementById("joinModalTitle");
const joinModalText = document.getElementById("joinModalText");
const joinModalTimer = document.getElementById("joinModalTimer");
const joinModalApprove = document.getElementById("joinModalApprove");
const joinModalReject = document.getElementById("joinModalReject");
const joinModalClose = document.getElementById("joinModalClose");
const joinModalTopClose = document.getElementById("joinModalTopClose");
const selfKickJokeModal = document.getElementById("selfKickJokeModal");
const selfKickJokeTitle = document.getElementById("selfKickJokeTitle");
const selfKickJokeText = document.getElementById("selfKickJokeText");
const selfKickJokeLobbyBtn = document.getElementById("selfKickJokeLobbyBtn");
const selfKickJokeCloseBtn = document.getElementById("selfKickJokeCloseBtn");
const kickSupervisorDeniedModal = document.getElementById("kickSupervisorDeniedModal");
const kickSupervisorDeniedTitle = document.getElementById("kickSupervisorDeniedTitle");
const kickSupervisorDeniedText = document.getElementById("kickSupervisorDeniedText");
const kickSupervisorDeniedOkBtn = document.getElementById("kickSupervisorDeniedOkBtn");
const kickSupervisorDeniedCloseBtn = document.getElementById("kickSupervisorDeniedCloseBtn");
const chatMessages = document.getElementById("chatMessages");
const chatForm = document.getElementById("chatForm");
const chatInput = document.getElementById("chatInput");
const sendBtn = document.getElementById("sendBtn");
const chatReplyDraft = document.getElementById("chatReplyDraft");
const chatReplyDraftLabel = document.getElementById("chatReplyDraftLabel");
const chatReplyDraftText = document.getElementById("chatReplyDraftText");
const chatReplyCancelBtn = document.getElementById("chatReplyCancelBtn");
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
const profileModalTopClose = document.getElementById("profileModalTopClose");
const profileAdminBox = document.getElementById("profileAdminBox");
const profileBanReasonLabel = document.getElementById("profileBanReasonLabel");
const profileBanReasonInput = document.getElementById("profileBanReasonInput");
const profileBanBtn = document.getElementById("profileBanBtn");
const profileUnbanBtn = document.getElementById("profileUnbanBtn");
const roomSupervisorToggle = document.getElementById("roomSupervisorToggle");
const roomSupervisorOverlay = document.getElementById("roomSupervisorOverlay");
const roomSupervisorPanel = document.getElementById("roomSupervisorPanel");
const roomSupervisorTitle = document.getElementById("roomSupervisorTitle");
const roomSupervisorClose = document.getElementById("roomSupervisorClose");
const roomTabAnnouncement = document.getElementById("roomTabAnnouncement");
const roomTabUsers = document.getElementById("roomTabUsers");
const roomTabAppeals = document.getElementById("roomTabAppeals");
const roomAnnouncementSection = document.getElementById("roomAnnouncementSection");
const roomAnnouncementTitle = document.getElementById("roomAnnouncementTitle");
const roomAnnouncementDesc = document.getElementById("roomAnnouncementDesc");
const roomAnnouncementInput = document.getElementById("roomAnnouncementInput");
const roomAnnouncementSend = document.getElementById("roomAnnouncementSend");
const roomUsersSection = document.getElementById("roomUsersSection");
const roomUsersTitle = document.getElementById("roomUsersTitle");
const roomUsersRefresh = document.getElementById("roomUsersRefresh");
const roomUsersDesc = document.getElementById("roomUsersDesc");
const roomUsersList = document.getElementById("roomUsersList");
const roomAppealsSection = document.getElementById("roomAppealsSection");
const roomAppealsTitle = document.getElementById("roomAppealsTitle");
const roomAppealsRefresh = document.getElementById("roomAppealsRefresh");
const roomAppealsDesc = document.getElementById("roomAppealsDesc");
const roomAppealsList = document.getElementById("roomAppealsList");
const videoTitle = document.getElementById("videoTitle");
const videoStatusText = document.getElementById("videoStatusText");
const videoHintText = document.getElementById("videoHintText");
const videoPlayerFrame = document.getElementById("videoPlayerFrame");
const roomVideoPlayer = document.getElementById("roomVideoPlayer");
const videoControlsBar = document.getElementById("videoControlsBar");
const videoPlayPauseBtn = document.getElementById("videoPlayPauseBtn");
const videoSeekRange = document.getElementById("videoSeekRange");
const videoTimeText = document.getElementById("videoTimeText");
const videoFullscreenBtn = document.getElementById("videoFullscreenBtn");
const videoLeaderTools = document.getElementById("videoLeaderTools");
const videoToolsToggle = document.getElementById("videoToolsToggle");
const videoToolsToggleIcon = videoToolsToggle ? videoToolsToggle.querySelector(".video-tools-toggle-icon") : null;
const videoToolsOverlay = document.getElementById("videoToolsOverlay");
const videoToolsClose = document.getElementById("videoToolsClose");
const videoToolsTitle = document.getElementById("videoToolsTitle");
const videoFileLabel = document.getElementById("videoFileLabel");
const videoFileInput = document.getElementById("videoFileInput");
const videoUploadBtn = document.getElementById("videoUploadBtn");

const query = new URLSearchParams(window.location.search);
const code = String(query.get("code") || "").trim().toUpperCase();
const encodedCode = encodeURIComponent(code);
const roomMobileLayoutQuery = window.matchMedia("(max-width: 980px)");

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
let roomSupervisorOpen = false;
let playersDrawerOpen = false;
let videoToolsOpen = false;
let roomSupervisorTab = "announcement";
let roomCachedAppeals = [];
let roomCachedUsers = [];
let globalAnnouncementOverlay = null;
let globalAnnouncementTitle = null;
let globalAnnouncementText = null;
let globalAnnouncementTimer = null;
let globalAnnouncementCountdown = 0;
let globalAnnouncementCountdownTimer = null;
let activeAnnouncementId = "";
let queuedAnnouncement = null;
let isSendingChatMessage = false;
let refreshMessagesInFlight = false;
let refreshMessagesQueued = false;
let retryableChatSend = null;
let activeReplyTarget = null;
const renderedMessageIds = new Set();
const CHAT_SEND_RETRY_WINDOW_MS = 30000;
const ROOM_VIDEO_MAX_BYTES = 80 * 1024 * 1024;
const ROOM_VIDEO_SYNC_LOCK_TOAST_MS = 4000;
const ROOM_VIDEO_LEADER_SEEK_DRIFT_SEC = 0.9;
const ROOM_VIDEO_MEMBER_HARD_SEEK_DRIFT_SEC = 1.6;
const ROOM_VIDEO_MEMBER_SOFT_DRIFT_SEC = 0.35;
const ROOM_VIDEO_MEMBER_CATCHUP_FACTOR = 0.08;
const ROOM_VIDEO_MEMBER_CATCHUP_LIMIT = 0.18;
const ROOM_VIDEO_CONTROLS_HIDE_DELAY_MS = 2200;
const ROOM_VIDEO_CONTROLS_AUTO_HIDE = true;
const FULLSCREEN_CHAT_NOTICE_TIMEOUT_MS = 2600;
let roomVideoState = null;
let roomVideoSyncState = null;
let activeRoomVideoId = "";
let roomVideoMetadataReady = false;
let suppressRoomVideoEvents = false;
let isUploadingRoomVideo = false;
let roomVideoSyncToastUntil = 0;
let roomVideoSeekDragging = false;
let roomVideoControlsHideTimer = null;
let fullscreenChatNoticeEl = null;
let fullscreenChatNoticeTimer = null;

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
    videoTitle: "فيديو الغرفة",
    videoNoVideo: "لا يوجد فيديو مرفوع بعد.",
    videoNowPlaying: "الفيديو الحالي: {name}",
    videoHint: "قائد الغرفة فقط يمكنه رفع ومزامنة الفيديو.",
    videoToolsTitle: "أدوات الفيديو",
    videoToolsOpenLabel: "فتح أدوات الفيديو",
    videoToolsCloseLabel: "إغلاق أدوات الفيديو",
    videoFileLabel: "اختيار فيديو من الجهاز",
    videoUploadBtn: "رفع الفيديو",
    videoUploadBusy: "جارٍ رفع الفيديو...",
    videoUploadNeedFile: "اختر ملف فيديو أولًا.",
    videoUploadSuccess: "تم رفع الفيديو وتحديث المزامنة.",
    videoUploadTooLarge: "حجم الفيديو كبير جدًا (الحد 80MB).",
    videoUploadType: "نوع الفيديو غير مدعوم. استخدم MP4 أو WebM أو OGG.",
    videoUploadNetworkError: "تعذر رفع الفيديو بسبب مشكلة اتصال. تأكد من الإنترنت وحاول مرة أخرى.",
    requestNetworkError: "تعذر الاتصال بالخادم. تأكد من الإنترنت وحاول مرة أخرى.",
    videoHostOnly: "التحكم بالفيديو متاح لقائد الغرفة فقط.",
    videoSyncFailed: "تعذر مزامنة الفيديو.",
    videoControlLocked: "المزامنة بيد قائد الغرفة.",
    videoIncomingMessage: "رسالة جديدة من {user}",
    videoPlayBtn: "تشغيل",
    videoPauseBtn: "إيقاف",
    videoFullscreenBtn: "ملء الشاشة",
    videoExitFullscreenBtn: "خروج",
    replyBtn: "رد",
    replyingTo: "الرد على {user}",
    replyCancel: "إلغاء الرد",
    playersTitle: "اللاعبون",
    playersDrawerBtn: "الأعضاء",
    playersDrawerOpenLabel: "فتح قائمة الأعضاء",
    playersDrawerCloseLabel: "إغلاق قائمة الأعضاء",
    roomMembersCount: "داخل الغرفة الآن: {count}",
    pendingRequestsTitle: "طلبات الانضمام",
    pendingEmpty: "لا توجد طلبات حالياً.",
    approveBtn: "قبول",
    rejectBtn: "رفض",
    closeBtn: "إغلاق",
    joinModalTitle: "طلب انضمام",
    joinModalPrompt: "{user} يريد الانضمام إلى الغرفة.",
    joinModalTimer: "إغلاق تلقائي خلال {seconds} ثانية",
    selfKickJokeTitle: "تنبيه",
    selfKickJokeText: "لا يمكنك طرد نفسك ايها الاحمق",
    selfKickJokeLobbyBtn: "العودة إلى اللوبي",
    kickSupervisorDeniedTitle: "مرفوض",
    kickSupervisorDeniedText: "لا يمكن طرد المشرف من الغرفة.",
    kickSupervisorDeniedOkBtn: "إغلاق",
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
    supervisorOpenBtn: "لوحة المشرف",
    supervisorSidebarTitle: "لوحة المشرف",
    supervisorTabAnnouncement: "الرسالة العامة",
    supervisorTabUsers: "كل الحسابات",
    supervisorTabAppeals: "طلبات رفع الحظر",
    supervisorAnnouncementDesc: "تظهر هذه الرسالة إجباريًا لكل المستخدمين لمدة 10 ثوانٍ.",
    supervisorAnnouncementPlaceholder: "اكتب الرسالة العامة هنا...",
    supervisorAnnouncementSendBtn: "إرسال الرسالة",
    supervisorAnnouncementNeedText: "اكتب نص الرسالة العامة أولًا.",
    supervisorAnnouncementDone: "تم إرسال الرسالة العامة.",
    supervisorUsersDesc: "قائمة كل الحسابات المسجلة (المتصلون أولاً).",
    supervisorAppealsDesc: "طلبات المستخدمين لفك الحظر من الموقع.",
    supervisorUsersRefreshBtn: "تحديث الحسابات",
    supervisorAppealsRefreshBtn: "تحديث الطلبات",
    supervisorAppealsEmpty: "لا توجد طلبات رفع حظر.",
    supervisorUsersEmpty: "لا توجد حسابات.",
    supervisorAppealBy: "المستخدم",
    supervisorAppealReason: "السبب",
    supervisorAppealStatus: "الحالة",
    supervisorAppealApprove: "قبول الطلب",
    supervisorAppealReject: "رفض الطلب",
    supervisorAppealApproved: "تم قبول الطلب.",
    supervisorAppealRejected: "تم رفض الطلب.",
    supervisorUserOnline: "متصل الآن",
    supervisorUserOffline: "غير متصل",
    supervisorUserCreated: "تاريخ الإنشاء",
    supervisorUserBanned: "محظور",
    supervisorUserNotBanned: "غير محظور",
    supervisorDeleteBtn: "حذف نهائي",
    supervisorDeleteConfirm: "تأكيد الحذف النهائي للحساب {user}؟ سيتم حذف كل بياناته.",
    supervisorDeleteDone: "تم حذف الحساب نهائيًا.",
    announcementModalTitle: "رسالة عامة من المشرف",
    announcementModalTimer: "ستختفي خلال {seconds} ثوانٍ",
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
    videoTitle: "Room Video",
    videoNoVideo: "No video uploaded yet.",
    videoNowPlaying: "Current video: {name}",
    videoHint: "Only the room leader can upload and sync video.",
    videoToolsTitle: "Video Tools",
    videoToolsOpenLabel: "Open video tools",
    videoToolsCloseLabel: "Close video tools",
    videoFileLabel: "Choose video from device",
    videoUploadBtn: "Upload Video",
    videoUploadBusy: "Uploading...",
    videoUploadNeedFile: "Choose a video file first.",
    videoUploadSuccess: "Video uploaded and sync updated.",
    videoUploadTooLarge: "Video is too large (max 80MB).",
    videoUploadType: "Unsupported video type. Use MP4, WebM, or OGG.",
    videoUploadNetworkError: "Video upload failed due to a network issue. Check your connection and try again.",
    requestNetworkError: "Could not reach the server. Check your connection and try again.",
    videoHostOnly: "Video controls are leader-only.",
    videoSyncFailed: "Failed to sync video.",
    videoControlLocked: "Video sync is controlled by room leader.",
    videoIncomingMessage: "New message from {user}",
    videoPlayBtn: "Play",
    videoPauseBtn: "Pause",
    videoFullscreenBtn: "Fullscreen",
    videoExitFullscreenBtn: "Exit",
    replyBtn: "Reply",
    replyingTo: "Replying to {user}",
    replyCancel: "Cancel reply",
    playersTitle: "Players",
    playersDrawerBtn: "Members",
    playersDrawerOpenLabel: "Open members list",
    playersDrawerCloseLabel: "Close members list",
    roomMembersCount: "Inside room now: {count}",
    pendingRequestsTitle: "Join Requests",
    pendingEmpty: "No pending requests.",
    approveBtn: "Approve",
    rejectBtn: "Reject",
    closeBtn: "Close",
    joinModalTitle: "Join Request",
    joinModalPrompt: "{user} wants to join this room.",
    joinModalTimer: "Auto close in {seconds}s",
    selfKickJokeTitle: "Warning",
    selfKickJokeText: "You cannot kick yourself, you fool.",
    selfKickJokeLobbyBtn: "Back to Lobby",
    kickSupervisorDeniedTitle: "Denied",
    kickSupervisorDeniedText: "You cannot kick a supervisor from this room.",
    kickSupervisorDeniedOkBtn: "Close",
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
    supervisorOpenBtn: "Supervisor Panel",
    supervisorSidebarTitle: "Supervisor Panel",
    supervisorTabAnnouncement: "Global Message",
    supervisorTabUsers: "All Accounts",
    supervisorTabAppeals: "Unban Requests",
    supervisorAnnouncementDesc: "This message is forced for all users for 10 seconds.",
    supervisorAnnouncementPlaceholder: "Write the global message here...",
    supervisorAnnouncementSendBtn: "Send Message",
    supervisorAnnouncementNeedText: "Please write the announcement text first.",
    supervisorAnnouncementDone: "Global message sent.",
    supervisorUsersDesc: "All registered accounts (online first).",
    supervisorAppealsDesc: "Users' requests to remove site bans.",
    supervisorUsersRefreshBtn: "Refresh Accounts",
    supervisorAppealsRefreshBtn: "Refresh Requests",
    supervisorAppealsEmpty: "No unban requests.",
    supervisorUsersEmpty: "No accounts found.",
    supervisorAppealBy: "User",
    supervisorAppealReason: "Reason",
    supervisorAppealStatus: "Status",
    supervisorAppealApprove: "Approve",
    supervisorAppealReject: "Reject",
    supervisorAppealApproved: "Request approved.",
    supervisorAppealRejected: "Request rejected.",
    supervisorUserOnline: "Online now",
    supervisorUserOffline: "Offline",
    supervisorUserCreated: "Created",
    supervisorUserBanned: "Banned",
    supervisorUserNotBanned: "Not banned",
    supervisorDeleteBtn: "Delete Forever",
    supervisorDeleteConfirm: "Confirm permanent deletion of {user}? All account data will be removed.",
    supervisorDeleteDone: "Account was permanently deleted.",
    announcementModalTitle: "Global Message From Supervisor",
    announcementModalTimer: "Closes in {seconds}s",
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
    renderedMessageIds.clear();
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

function createClientMessageId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID().replace(/-/g, "");
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
}

function reserveClientMessageId(text, replyToMessageId = 0) {
  const now = Date.now();
  if (
    retryableChatSend &&
    retryableChatSend.text === text &&
    Number(retryableChatSend.replyToMessageId || 0) === Number(replyToMessageId || 0) &&
    now - retryableChatSend.timestamp <= CHAT_SEND_RETRY_WINDOW_MS
  ) {
    return retryableChatSend.id;
  }
  const id = createClientMessageId();
  retryableChatSend = {
    id,
    text,
    replyToMessageId: Number(replyToMessageId || 0),
    timestamp: now
  };
  return id;
}

function setChatSendingState(sending) {
  isSendingChatMessage = Boolean(sending);
  if (sendBtn) {
    sendBtn.disabled = isSendingChatMessage;
  }
}

function shortenReplyText(text, max = 90) {
  const clean = String(text || "").replace(/\s+/g, " ").trim();
  if (!clean) {
    return "";
  }
  if (clean.length <= max) {
    return clean;
  }
  return `${clean.slice(0, max - 1)}…`;
}

function replyTargetDisplayName(username) {
  if (!username) {
    return "-";
  }
  if (username === me) {
    return t("youLabel");
  }
  return displayNameFor(username);
}

function clearReplyTarget() {
  activeReplyTarget = null;
  renderReplyDraft();
}

function renderReplyDraft() {
  if (!chatReplyDraft || !chatReplyDraftLabel || !chatReplyDraftText) {
    return;
  }
  if (!activeReplyTarget) {
    chatReplyDraft.classList.add("hidden");
    chatReplyDraftLabel.textContent = "";
    chatReplyDraftText.textContent = "";
    if (chatReplyCancelBtn) {
      chatReplyCancelBtn.title = t("replyCancel");
      chatReplyCancelBtn.setAttribute("aria-label", t("replyCancel"));
    }
    return;
  }
  chatReplyDraft.classList.remove("hidden");
  chatReplyDraftLabel.textContent = fmt(t("replyingTo"), {
    user: replyTargetDisplayName(activeReplyTarget.user)
  });
  chatReplyDraftText.textContent = shortenReplyText(activeReplyTarget.text);
  if (chatReplyCancelBtn) {
    chatReplyCancelBtn.title = t("replyCancel");
    chatReplyCancelBtn.setAttribute("aria-label", t("replyCancel"));
  }
}

function setReplyTargetFromMessage(message) {
  if (!message || message.type !== "user") {
    return;
  }
  const messageId = Number(message.id || 0);
  if (!messageId) {
    return;
  }
  activeReplyTarget = {
    id: messageId,
    user: String(message.user || ""),
    text: String(message.text || "")
  };
  renderReplyDraft();
  chatInput.focus();
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
  const toastText = document.createElement("span");
  toastText.className = "toast-text";
  toastText.textContent = text;
  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "toast-close";
  closeBtn.textContent = "✕";
  closeBtn.setAttribute("aria-label", t("closeBtn"));
  closeBtn.title = t("closeBtn");
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
    closeBtn.setAttribute("aria-label", cancelText || t("closeBtn"));
    closeBtn.title = cancelText || t("closeBtn");

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
    cancelBtn.textContent = cancelText || t("closeBtn");

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

function renderRoomMembersCount(count = 0) {
  const safeCount = Number(count || 0);
  roomMembersCountText.textContent = fmt(t("roomMembersCount"), { count: safeCount });
  if (playersDrawerCountBadge) {
    playersDrawerCountBadge.textContent = String(safeCount);
  }
}

function isRoomMobileLayout() {
  return Boolean(roomMobileLayoutQuery?.matches);
}

function syncRoomBodyLock() {
  const shouldLock = Boolean(roomSupervisorOpen || playersDrawerOpen || videoToolsOpen);
  document.body.classList.toggle("supervisor-menu-open", shouldLock);
  document.body.classList.toggle("members-drawer-open", Boolean(playersDrawerOpen));
}

function setPlayersDrawerOpen(open) {
  const canOpen = isRoomMobileLayout();
  const nextOpen = Boolean(open && canOpen);
  playersDrawerOpen = nextOpen;
  if (playersDrawer) {
    playersDrawer.classList.toggle("hidden", canOpen && !nextOpen);
    playersDrawer.classList.toggle("is-open", nextOpen);
    playersDrawer.setAttribute("aria-hidden", canOpen ? (nextOpen ? "false" : "true") : "false");
  }
  if (playersDrawerOverlay) {
    playersDrawerOverlay.classList.toggle("hidden", !nextOpen);
    playersDrawerOverlay.classList.toggle("is-open", nextOpen);
    playersDrawerOverlay.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  }
  if (playersDrawerToggle) {
    playersDrawerToggle.classList.toggle("is-open", nextOpen);
    playersDrawerToggle.setAttribute("aria-expanded", nextOpen ? "true" : "false");
    const toggleLabel = nextOpen ? t("playersDrawerCloseLabel") : t("playersDrawerOpenLabel");
    playersDrawerToggle.setAttribute("aria-label", toggleLabel);
    playersDrawerToggle.title = toggleLabel;
  }
  if (nextOpen) {
    setVideoToolsDrawerOpen(false);
  }
  syncRoomBodyLock();
}

function setVideoToolsDrawerOpen(open) {
  const mobile = isRoomMobileLayout();
  const canOpen = mobile && isRoomLeader();
  const nextOpen = Boolean(open && canOpen);
  videoToolsOpen = nextOpen;
  if (videoLeaderTools) {
    if (mobile) {
      videoLeaderTools.classList.toggle("is-open", nextOpen);
      videoLeaderTools.classList.toggle("hidden", !canOpen);
      videoLeaderTools.setAttribute("aria-hidden", canOpen ? (nextOpen ? "false" : "true") : "true");
    } else {
      videoLeaderTools.classList.remove("is-open");
      videoLeaderTools.setAttribute("aria-hidden", "false");
    }
  }
  if (videoToolsOverlay) {
    videoToolsOverlay.classList.toggle("hidden", !nextOpen);
    videoToolsOverlay.classList.toggle("is-open", nextOpen);
    videoToolsOverlay.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  }
  if (videoToolsToggle) {
    videoToolsToggle.classList.toggle("is-open", nextOpen);
    videoToolsToggle.classList.toggle("hidden", !canOpen || !mobile);
    videoToolsToggle.setAttribute("aria-expanded", nextOpen ? "true" : "false");
    const label = nextOpen ? t("videoToolsCloseLabel") : t("videoToolsOpenLabel");
    videoToolsToggle.setAttribute("aria-label", label);
    videoToolsToggle.title = label;
  }
  if (videoToolsToggleIcon) {
    videoToolsToggleIcon.textContent = nextOpen ? "X" : "+";
  }
  if (nextOpen) {
    setPlayersDrawerOpen(false);
  }
  syncRoomBodyLock();
}

function syncPlayersDrawerMode() {
  if (!playersDrawerToggle) {
    return;
  }
  const mobile = isRoomMobileLayout();
  playersDrawerToggle.classList.toggle("hidden", !mobile);
  if (!mobile) {
    playersDrawerOpen = false;
    videoToolsOpen = false;
    if (playersDrawer) {
      playersDrawer.classList.remove("hidden", "is-open");
      playersDrawer.setAttribute("aria-hidden", "false");
    }
    if (playersDrawerOverlay) {
      playersDrawerOverlay.classList.add("hidden");
      playersDrawerOverlay.classList.remove("is-open");
      playersDrawerOverlay.setAttribute("aria-hidden", "true");
    }
    if (videoLeaderTools) {
      videoLeaderTools.classList.remove("is-open");
      videoLeaderTools.setAttribute("aria-hidden", "false");
    }
    if (videoToolsOverlay) {
      videoToolsOverlay.classList.add("hidden");
      videoToolsOverlay.classList.remove("is-open");
      videoToolsOverlay.setAttribute("aria-hidden", "true");
    }
    if (videoToolsToggle) {
      videoToolsToggle.classList.add("hidden");
      videoToolsToggle.classList.remove("is-open");
      videoToolsToggle.setAttribute("aria-expanded", "false");
    }
    if (videoToolsToggleIcon) {
      videoToolsToggleIcon.textContent = "+";
    }
    syncRoomBodyLock();
    return;
  }
  setPlayersDrawerOpen(playersDrawerOpen);
  setVideoToolsDrawerOpen(videoToolsOpen);
}

function isRoomLeader() {
  return Boolean(me && host && me === host);
}

function formatVideoClock(value) {
  const total = Math.max(0, Math.floor(Number(value) || 0));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) {
    return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function getRoomVideoDuration() {
  if (!roomVideoPlayer) {
    return Number(roomVideoState?.duration || 0);
  }
  const nativeDuration = Number(roomVideoPlayer.duration);
  if (Number.isFinite(nativeDuration) && nativeDuration > 0) {
    return nativeDuration;
  }
  const fallbackDuration = Number(roomVideoState?.duration || 0);
  if (Number.isFinite(fallbackDuration) && fallbackDuration > 0) {
    return fallbackDuration;
  }
  return 0;
}

function isVideoFrameFullscreen() {
  if (roomVideoPlayer && roomVideoPlayer.webkitDisplayingFullscreen) {
    return true;
  }
  if (!videoPlayerFrame || !document.fullscreenElement) {
    return false;
  }
  return document.fullscreenElement === videoPlayerFrame || videoPlayerFrame.contains(document.fullscreenElement);
}

async function lockLandscapeForMobileFullscreen() {
  try {
    if (!window.matchMedia || !window.matchMedia("(max-width: 980px)").matches) {
      return;
    }
    const orientationApi = screen && screen.orientation;
    if (!orientationApi || typeof orientationApi.lock !== "function") {
      return;
    }
    await orientationApi.lock("landscape");
  } catch (_error) {
    // Best effort only; ignore unsupported/blocked orientation lock.
  }
}

async function unlockOrientationAfterFullscreen() {
  try {
    const orientationApi = screen && screen.orientation;
    if (!orientationApi || typeof orientationApi.unlock !== "function") {
      return;
    }
    orientationApi.unlock();
  } catch (_error) {
    // Best effort only.
  }
}

function clearFullscreenChatNoticeTimer() {
  if (!fullscreenChatNoticeTimer) {
    return;
  }
  clearTimeout(fullscreenChatNoticeTimer);
  fullscreenChatNoticeTimer = null;
}

function ensureFullscreenChatNotice() {
  if (!videoPlayerFrame) {
    return null;
  }
  if (fullscreenChatNoticeEl) {
    return fullscreenChatNoticeEl;
  }
  const el = document.createElement("div");
  el.className = "fullscreen-chat-notice";
  el.setAttribute("aria-live", "polite");
  el.setAttribute("aria-atomic", "true");
  videoPlayerFrame.appendChild(el);
  fullscreenChatNoticeEl = el;
  return el;
}

function hideFullscreenChatNotice() {
  clearFullscreenChatNoticeTimer();
  if (!fullscreenChatNoticeEl) {
    return;
  }
  fullscreenChatNoticeEl.classList.remove("show");
}

function showFullscreenChatNotice(message) {
  if (!message || message.type !== "user" || !message.user || message.user === me) {
    return;
  }
  if (!isVideoFrameFullscreen()) {
    return;
  }
  const notice = ensureFullscreenChatNotice();
  if (!notice) {
    return;
  }
  const sender = displayNameFor(message.user);
  const fullText = String(message.text || "").trim();
  notice.textContent = `${fmt(t("videoIncomingMessage"), { user: sender })}: ${fullText}`;
  notice.classList.add("show");
  clearFullscreenChatNoticeTimer();
  fullscreenChatNoticeTimer = setTimeout(() => {
    hideFullscreenChatNotice();
  }, FULLSCREEN_CHAT_NOTICE_TIMEOUT_MS);
}

function clearRoomVideoControlsHideTimer() {
  if (!roomVideoControlsHideTimer) {
    return;
  }
  clearTimeout(roomVideoControlsHideTimer);
  roomVideoControlsHideTimer = null;
}

function setRoomVideoControlsVisible(visible) {
  if (!videoPlayerFrame) {
    return;
  }
  videoPlayerFrame.classList.toggle("controls-visible", Boolean(visible));
}

function shouldAutoHideRoomVideoControls() {
  if (!ROOM_VIDEO_CONTROLS_AUTO_HIDE || !roomVideoPlayer || !roomVideoState) {
    return false;
  }
  if (roomVideoSeekDragging) {
    return false;
  }
  return !roomVideoPlayer.paused && !roomVideoPlayer.ended;
}

function scheduleRoomVideoControlsAutoHide() {
  clearRoomVideoControlsHideTimer();
  if (!shouldAutoHideRoomVideoControls()) {
    setRoomVideoControlsVisible(true);
    return;
  }
  roomVideoControlsHideTimer = setTimeout(() => {
    if (!shouldAutoHideRoomVideoControls()) {
      setRoomVideoControlsVisible(true);
      return;
    }
    setRoomVideoControlsVisible(false);
  }, ROOM_VIDEO_CONTROLS_HIDE_DELAY_MS);
}

function revealRoomVideoControls() {
  setRoomVideoControlsVisible(true);
  scheduleRoomVideoControlsAutoHide();
}

function updateRoomVideoControls({ previewTime = null } = {}) {
  const hasVideo = Boolean(roomVideoState && roomVideoState.src);
  const duration = hasVideo ? getRoomVideoDuration() : 0;
  let currentTime = hasVideo ? clampVideoTime(roomVideoPlayer?.currentTime, duration) : 0;
  if (previewTime !== null && Number.isFinite(previewTime)) {
    currentTime = clampVideoTime(previewTime, duration);
  }

  if (videoControlsBar) {
    videoControlsBar.classList.toggle("is-disabled", !hasVideo);
  }

  if (videoSeekRange) {
    if (!roomVideoSeekDragging || previewTime !== null || !hasVideo) {
      const progress = duration > 0 ? Math.round((currentTime / duration) * 1000) : 0;
      videoSeekRange.value = String(Math.max(0, Math.min(1000, progress)));
    }
    const progressPercent = duration > 0 ? Math.max(0, Math.min(100, (currentTime / duration) * 100)) : 0;
    videoSeekRange.style.setProperty("--video-progress", `${progressPercent}%`);
    videoSeekRange.disabled = !hasVideo || duration <= 0;
  }

  if (videoTimeText) {
    videoTimeText.textContent = `${formatVideoClock(currentTime)} / ${formatVideoClock(duration)}`;
  }

  if (videoPlayPauseBtn) {
    const isPlaying = hasVideo && roomVideoPlayer && !roomVideoPlayer.paused && !roomVideoPlayer.ended;
    videoPlayPauseBtn.disabled = !hasVideo;
    videoPlayPauseBtn.textContent = isPlaying ? "❚❚" : "▶";
    videoPlayPauseBtn.setAttribute("aria-label", isPlaying ? t("videoPauseBtn") : t("videoPlayBtn"));
    videoPlayPauseBtn.title = isPlaying ? t("videoPauseBtn") : t("videoPlayBtn");
  }

  if (videoFullscreenBtn) {
    videoFullscreenBtn.disabled = !hasVideo;
    const isFullscreen = isVideoFrameFullscreen();
    videoFullscreenBtn.textContent = isFullscreen ? "⤢" : "⛶";
    videoFullscreenBtn.setAttribute("aria-label", isFullscreen ? t("videoExitFullscreenBtn") : t("videoFullscreenBtn"));
    videoFullscreenBtn.title = isFullscreen ? t("videoExitFullscreenBtn") : t("videoFullscreenBtn");
  }

  if (!hasVideo || !shouldAutoHideRoomVideoControls()) {
    clearRoomVideoControlsHideTimer();
    setRoomVideoControlsVisible(true);
  }
  if (!isVideoFrameFullscreen()) {
    hideFullscreenChatNotice();
  }
}

function normalizeVideoRate(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 1;
  }
  return Math.min(3, Math.max(0.25, numeric));
}

function clampVideoTime(value, duration = 0) {
  const numeric = Number(value);
  const safe = Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
  if (duration > 0) {
    return Math.min(duration, safe);
  }
  return safe;
}

function normalizeIncomingRoomVideoSync(sync, duration = 0) {
  if (!sync) {
    return null;
  }
  return {
    ...sync,
    currentTime: clampVideoTime(sync.currentTime, duration),
    playbackRate: normalizeVideoRate(sync.playbackRate),
    snapshotAt: Date.now()
  };
}

function computeRoomVideoTargetTime(sync, duration = 0) {
  if (!sync) {
    return 0;
  }
  const rate = normalizeVideoRate(sync.playbackRate);
  let currentTime = clampVideoTime(sync.currentTime, duration);
  if (sync.playing) {
    const anchor = Number(sync.snapshotAt || Date.now());
    const elapsed = Math.max(0, (Date.now() - anchor) / 1000);
    currentTime += elapsed * rate;
  }
  return clampVideoTime(currentTime, duration);
}

function computeFollowerCatchupRate(baseRate, driftSec) {
  const offset = Math.max(
    -ROOM_VIDEO_MEMBER_CATCHUP_LIMIT,
    Math.min(ROOM_VIDEO_MEMBER_CATCHUP_LIMIT, driftSec * ROOM_VIDEO_MEMBER_CATCHUP_FACTOR)
  );
  return normalizeVideoRate(baseRate + offset);
}

function withSuppressedRoomVideoEvents(callback) {
  suppressRoomVideoEvents = true;
  try {
    callback();
  } finally {
    setTimeout(() => {
      suppressRoomVideoEvents = false;
    }, 0);
  }
}

function setVideoUploadBusy(busy) {
  isUploadingRoomVideo = Boolean(busy);
  if (videoUploadBtn) {
    videoUploadBtn.disabled = isUploadingRoomVideo;
    videoUploadBtn.textContent = isUploadingRoomVideo ? t("videoUploadBusy") : t("videoUploadBtn");
  }
  if (videoFileInput) {
    videoFileInput.disabled = isUploadingRoomVideo;
  }
}

function showVideoControlLockedToast() {
  const now = Date.now();
  if (now < roomVideoSyncToastUntil) {
    return;
  }
  roomVideoSyncToastUntil = now + ROOM_VIDEO_SYNC_LOCK_TOAST_MS;
  showToast(t("videoControlLocked"));
}

function clearRoomVideoPlayer() {
  if (!roomVideoPlayer) {
    return;
  }
  roomVideoSeekDragging = false;
  clearRoomVideoControlsHideTimer();
  withSuppressedRoomVideoEvents(() => {
    roomVideoPlayer.pause();
    roomVideoPlayer.removeAttribute("src");
    roomVideoPlayer.load();
  });
  activeRoomVideoId = "";
  roomVideoMetadataReady = false;
  roomVideoState = null;
  roomVideoSyncState = null;
  hideFullscreenChatNotice();
  setRoomVideoControlsVisible(true);
  updateRoomVideoControls();
}

async function applyRoomVideoSyncToPlayer({ forceSeek = false } = {}) {
  if (!roomVideoPlayer || !roomVideoState || !roomVideoSyncState) {
    return;
  }
  suppressRoomVideoEvents = true;
  const duration = Number.isFinite(roomVideoPlayer.duration) && roomVideoPlayer.duration > 0
    ? Number(roomVideoPlayer.duration)
    : Number(roomVideoState.duration || 0);
  const targetTime = computeRoomVideoTargetTime(roomVideoSyncState, duration);
  const baseRate = normalizeVideoRate(roomVideoSyncState.playbackRate);
  const localTime = clampVideoTime(roomVideoPlayer.currentTime, duration);
  const drift = targetTime - localTime;
  const absDrift = Math.abs(drift);
  const leaderControl = isRoomLeader();
  const hardSeekThreshold = leaderControl ? ROOM_VIDEO_LEADER_SEEK_DRIFT_SEC : ROOM_VIDEO_MEMBER_HARD_SEEK_DRIFT_SEC;
  const shouldHardSeek = forceSeek || absDrift > hardSeekThreshold;
  const allowSoftCatchup =
    !leaderControl &&
    roomVideoSyncState.playing &&
    !shouldHardSeek &&
    absDrift > ROOM_VIDEO_MEMBER_SOFT_DRIFT_SEC;
  const desiredRate = allowSoftCatchup
    ? computeFollowerCatchupRate(baseRate, drift)
    : baseRate;

  try {
    if (Math.abs(Number(roomVideoPlayer.playbackRate || 1) - desiredRate) > 0.01) {
      roomVideoPlayer.playbackRate = desiredRate;
    }
    if (shouldHardSeek) {
      roomVideoPlayer.currentTime = targetTime;
    }

    if (roomVideoSyncState.playing) {
      try {
        await roomVideoPlayer.play();
      } catch (_error) {
        // Autoplay may be blocked until user interaction.
      }
    } else if (!roomVideoPlayer.paused) {
      roomVideoPlayer.pause();
    }
  } finally {
    setTimeout(() => {
      suppressRoomVideoEvents = false;
    }, 0);
    updateRoomVideoControls();
  }
}

function renderRoomVideo(room) {
  if (!videoLeaderTools || !videoStatusText || !videoHintText || !roomVideoPlayer) {
    return;
  }

  const canControl = isRoomLeader();
  if (!isRoomMobileLayout()) {
    videoLeaderTools.classList.toggle("hidden", !canControl);
  }
  if (!canControl) {
    setVideoToolsDrawerOpen(false);
  } else {
    setVideoToolsDrawerOpen(videoToolsOpen);
  }
  videoHintText.textContent = t("videoHint");

  const nextVideo = room?.video && room.video.src ? room.video : null;
  if (!nextVideo) {
    videoStatusText.textContent = t("videoNoVideo");
    clearRoomVideoPlayer();
    return;
  }

  roomVideoState = nextVideo;
  const incomingDuration = Number(nextVideo.duration || 0);
  roomVideoSyncState = normalizeIncomingRoomVideoSync(nextVideo.sync, incomingDuration);
  const nextVideoId = String(nextVideo.id || "");
  const sourceChanged = activeRoomVideoId !== nextVideoId || roomVideoPlayer.getAttribute("src") !== nextVideo.src;
  activeRoomVideoId = nextVideoId;

  if (sourceChanged) {
    roomVideoMetadataReady = false;
    withSuppressedRoomVideoEvents(() => {
      roomVideoPlayer.pause();
      roomVideoPlayer.src = nextVideo.src;
      roomVideoPlayer.load();
      roomVideoPlayer.playbackRate = 1;
      roomVideoPlayer.currentTime = 0;
    });
    revealRoomVideoControls();
  }

  const fileLabel = String(nextVideo.filename || "video");
  videoStatusText.textContent = fmt(t("videoNowPlaying"), { name: fileLabel });
  updateRoomVideoControls();
  if (!roomVideoSyncState) {
    return;
  }
  if (roomVideoMetadataReady || roomVideoPlayer.readyState >= 1) {
    applyRoomVideoSyncToPlayer({ forceSeek: sourceChanged });
  }
}

async function readVideoDurationFromFile(file) {
  return new Promise((resolve) => {
    const probe = document.createElement("video");
    const objectUrl = URL.createObjectURL(file);
    const cleanup = () => {
      URL.revokeObjectURL(objectUrl);
      probe.removeAttribute("src");
      probe.load();
    };
    probe.preload = "metadata";
    probe.onloadedmetadata = () => {
      const value = Number(probe.duration || 0);
      cleanup();
      resolve(Number.isFinite(value) && value > 0 ? value : 0);
    };
    probe.onerror = () => {
      cleanup();
      resolve(0);
    };
    probe.src = objectUrl;
  });
}

async function uploadRoomVideo() {
  if (!isRoomLeader()) {
    showToast(t("videoHostOnly"));
    return;
  }
  const file = videoFileInput?.files?.[0];
  if (!file) {
    showToast(t("videoUploadNeedFile"));
    return;
  }
  if (Number(file.size || 0) > ROOM_VIDEO_MAX_BYTES) {
    showToast(t("videoUploadTooLarge"));
    return;
  }
  const cleanType = String(file.type || "").trim().toLowerCase();
  if (cleanType && !["video/mp4", "video/webm", "video/ogg"].includes(cleanType)) {
    showToast(t("videoUploadType"));
    return;
  }

  const duration = await readVideoDurationFromFile(file);
  const formData = new FormData();
  formData.append("video", file, file.name);
  if (duration > 0) {
    formData.append("duration", String(duration));
  }

  setVideoUploadBusy(true);
  try {
    const result = await uploadRoomVideoRequest(`/api/rooms/${encodedCode}/video`, formData);
    videoFileInput.value = "";
    showToast(t("videoUploadSuccess"), "success");
    if (result?.room) {
      renderRoomInfo(result.room);
    }
    await refreshMessages();
  } catch (error) {
    if (error.code === "NETWORK_ERROR") {
      showToast(t("videoUploadNetworkError"));
      return;
    }
    if (error.code === "VIDEO_TOO_LARGE") {
      showToast(t("videoUploadTooLarge"));
      return;
    }
    if (error.code === "VIDEO_INVALID_TYPE") {
      showToast(t("videoUploadType"));
      return;
    }
    showToast(error.message || t("videoSyncFailed"));
  } finally {
    setVideoUploadBusy(false);
  }
}

function uploadRoomVideoRequest(pathname, formData) {
  return new Promise((resolve, reject) => {
    const token = getToken();
    const xhr = new XMLHttpRequest();
    xhr.open("POST", pathname, true);
    xhr.responseType = "json";
    xhr.timeout = 180000;
    xhr.setRequestHeader("X-Lang", getLang());
    if (token) {
      xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    }

    xhr.onload = () => {
      const payload =
        xhr.response && typeof xhr.response === "object"
          ? xhr.response
          : (() => {
              try {
                return JSON.parse(xhr.responseText || "{}");
              } catch (_error) {
                return {};
              }
            })();
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(payload);
        return;
      }
      const error = new Error(payload.error || t("toastRequestFailed"));
      error.status = xhr.status;
      error.code = payload.code || "";
      error.data = payload;
      reject(error);
    };

    xhr.onerror = () => {
      const error = new Error(t("requestNetworkError"));
      error.status = 0;
      error.code = "NETWORK_ERROR";
      reject(error);
    };

    xhr.onabort = () => {
      const error = new Error(t("requestNetworkError"));
      error.status = 0;
      error.code = "NETWORK_ERROR";
      reject(error);
    };

    xhr.ontimeout = () => {
      const error = new Error(t("requestNetworkError"));
      error.status = 0;
      error.code = "NETWORK_ERROR";
      reject(error);
    };

    xhr.send(formData);
  });
}

async function sendRoomVideoSync(action) {
  if (!isRoomLeader() || !roomVideoState || !activeRoomVideoId || !roomVideoPlayer) {
    if (!isRoomLeader()) {
      showVideoControlLockedToast();
    }
    return;
  }
  try {
    const duration = Number.isFinite(roomVideoPlayer.duration) && roomVideoPlayer.duration > 0
      ? Number(roomVideoPlayer.duration)
      : undefined;
    const payload = {
      action,
      videoId: activeRoomVideoId,
      currentTime: Number(roomVideoPlayer.currentTime || 0),
      playbackRate: normalizeVideoRate(roomVideoPlayer.playbackRate),
      duration
    };
    const result = await api(`/api/rooms/${encodedCode}/video-sync`, {
      method: "POST",
      body: payload
    });
    if (result?.room) {
      renderRoomInfo(result.room);
    }
  } catch (error) {
    showToast(error.message || t("videoSyncFailed"));
  }
}

function handleRoomVideoControlEvent(action) {
  if (suppressRoomVideoEvents || !roomVideoState) {
    return;
  }
  if (!isRoomLeader()) {
    applyRoomVideoSyncToPlayer({ forceSeek: true });
    showVideoControlLockedToast();
    return;
  }
  sendRoomVideoSync(action);
}

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
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
  setRoomSupervisorOpen(false);
  hideGlobalAnnouncement();
  queuedAnnouncement = null;
  clearReplyTarget();
  closeRequestModal();
  clearRoomVideoPlayer();
  const suffix = message ? `?msg=${encodeURIComponent(message)}` : "";
  window.location.href = `/${suffix}`;
}

async function api(pathname, options = {}) {
  const token = getToken();
  const isFormDataBody = typeof FormData !== "undefined" && options.body instanceof FormData;
  const headers = {
    "X-Lang": getLang(),
    ...(options.headers || {})
  };
  if (!isFormDataBody && options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  let response;
  try {
    response = await fetch(pathname, {
      method: options.method || "GET",
      headers,
      body: options.body
        ? (isFormDataBody ? options.body : JSON.stringify(options.body))
        : undefined
    });
  } catch (error) {
    const wrapped = new Error(t("requestNetworkError"));
    wrapped.status = 0;
    wrapped.code = "NETWORK_ERROR";
    wrapped.cause = error;
    throw wrapped;
  }

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

function roomAdminInfoRow(label, value) {
  const row = document.createElement("p");
  row.className = "muted room-line";
  row.textContent = `${label}: ${value}`;
  return row;
}

function setRoomSupervisorOpen(open) {
  const canOpen = isSupervisor;
  const nextOpen = Boolean(open && canOpen);
  roomSupervisorOpen = nextOpen;
  roomSupervisorOverlay.classList.toggle("hidden", !nextOpen);
  roomSupervisorPanel.classList.toggle("hidden", !nextOpen);
  roomSupervisorOverlay.classList.toggle("is-open", nextOpen);
  roomSupervisorPanel.classList.toggle("is-open", nextOpen);
  roomSupervisorOverlay.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  roomSupervisorPanel.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  roomSupervisorToggle.setAttribute("aria-expanded", nextOpen ? "true" : "false");
  syncRoomBodyLock();
  if (nextOpen) {
    setPlayersDrawerOpen(false);
    setVideoToolsDrawerOpen(false);
    setRoomSupervisorTab(roomSupervisorTab, false);
    refreshRoomSupervisorUsers();
    refreshRoomSupervisorAppeals();
  }
}

function setRoomSupervisorTab(tabName, scrollIntoView = true) {
  const allowed = ["announcement", "users", "appeals"];
  roomSupervisorTab = allowed.includes(tabName) ? tabName : "announcement";
  roomTabAnnouncement.classList.toggle("active", roomSupervisorTab === "announcement");
  roomTabUsers.classList.toggle("active", roomSupervisorTab === "users");
  roomTabAppeals.classList.toggle("active", roomSupervisorTab === "appeals");
  if (!scrollIntoView) {
    return;
  }
  const target = roomSupervisorTab === "announcement"
    ? roomAnnouncementSection
    : roomSupervisorTab === "users"
      ? roomUsersSection
      : roomAppealsSection;
  target.scrollIntoView({ behavior: "smooth", block: "start" });
  target.classList.add("section-focus-flash");
  setTimeout(() => target.classList.remove("section-focus-flash"), 900);
}

function syncRoomSupervisorControls() {
  roomSupervisorToggle.classList.toggle("hidden", !isSupervisor);
  if (!isSupervisor) {
    setRoomSupervisorOpen(false);
  }
}

function renderRoomSupervisorAppeals(requests) {
  roomCachedAppeals = requests.slice();
  roomAppealsList.innerHTML = "";
  if (requests.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("supervisorAppealsEmpty");
    roomAppealsList.appendChild(empty);
    return;
  }
  requests.forEach((item) => {
    const card = document.createElement("article");
    card.className = "room-card";
    const title = document.createElement("h4");
    title.textContent = `${t("supervisorAppealBy")}: ${item.username}`;
    card.appendChild(title);
    card.appendChild(roomAdminInfoRow(t("supervisorAppealReason"), item.reason || "-"));
    card.appendChild(roomAdminInfoRow(t("supervisorAppealStatus"), item.status || "pending"));
    if (item.status === "pending") {
      const actions = document.createElement("div");
      actions.className = "room-actions";
      const approveBtn = document.createElement("button");
      approveBtn.type = "button";
      approveBtn.className = "btn btn-approve";
      approveBtn.textContent = t("supervisorAppealApprove");
      approveBtn.addEventListener("click", async () => {
        try {
          await api("/api/admin/ban-appeals/decision", {
            method: "POST",
            body: { username: item.username, action: "approve", note: "Approved by supervisor" }
          });
          showToast(t("supervisorAppealApproved"), "success");
          refreshRoomSupervisorAppeals();
          refreshRoomSupervisorUsers();
        } catch (error) {
          showToast(error.message);
        }
      });
      const rejectBtn = document.createElement("button");
      rejectBtn.type = "button";
      rejectBtn.className = "btn btn-reject";
      rejectBtn.textContent = t("supervisorAppealReject");
      rejectBtn.addEventListener("click", async () => {
        try {
          await api("/api/admin/ban-appeals/decision", {
            method: "POST",
            body: { username: item.username, action: "reject", note: "Rejected by supervisor" }
          });
          showToast(t("supervisorAppealRejected"), "success");
          refreshRoomSupervisorAppeals();
        } catch (error) {
          showToast(error.message);
        }
      });
      actions.appendChild(approveBtn);
      actions.appendChild(rejectBtn);
      card.appendChild(actions);
    }
    roomAppealsList.appendChild(card);
  });
}

function renderRoomSupervisorUsers(users) {
  roomCachedUsers = users.slice();
  roomUsersList.innerHTML = "";
  if (users.length === 0) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = t("supervisorUsersEmpty");
    roomUsersList.appendChild(empty);
    return;
  }
  users.forEach((item) => {
    const card = document.createElement("article");
    card.className = "admin-user-card";
    const title = document.createElement("h4");
    title.className = "admin-user-title";
    title.textContent = `@${item.username}`;
    card.appendChild(title);
    card.appendChild(roomAdminInfoRow(item.displayName || item.username, item.isOnline ? t("supervisorUserOnline") : t("supervisorUserOffline")));
    card.appendChild(roomAdminInfoRow(t("supervisorUserCreated"), formatDate(item.createdAt)));
    card.appendChild(roomAdminInfoRow(t("supervisorAppealStatus"), item.isBanned ? t("supervisorUserBanned") : t("supervisorUserNotBanned")));
    if (!item.isSupervisor) {
      const actions = document.createElement("div");
      actions.className = "room-actions";
      const banOrUnbanBtn = document.createElement("button");
      banOrUnbanBtn.type = "button";
      banOrUnbanBtn.className = item.isBanned ? "btn btn-approve" : "btn btn-reject";
      banOrUnbanBtn.textContent = item.isBanned ? t("supervisorUnbanBtn") : t("supervisorBanBtn");
      banOrUnbanBtn.addEventListener("click", async () => {
        try {
          if (item.isBanned) {
            await api("/api/admin/unban-user", {
              method: "POST",
              body: { username: item.username, note: "Unbanned from supervisor panel" }
            });
            showToast(t("supervisorUnbanDone"), "success");
          } else {
            const reason = await openActionDialog({
              title: t("supervisorBanBtn"),
              message: t("supervisorNeedReason"),
              input: true,
              inputPlaceholder: t("supervisorNeedReason"),
              confirmText: t("supervisorBanBtn"),
              cancelText: t("closeBtn"),
              confirmClass: "btn btn-reject"
            });
            if (reason === null) {
              return;
            }
            if (reason.length < 3) {
              if (reason.length > 0) {
                showToast(t("supervisorNeedReason"));
              }
              return;
            }
            await api("/api/admin/ban-user", {
              method: "POST",
              body: { username: item.username, reason }
            });
            showToast(t("supervisorBanDone"), "success");
          }
          refreshRoomSupervisorUsers();
        } catch (error) {
          showToast(error.message);
        }
      });
      const deleteBtn = document.createElement("button");
      deleteBtn.type = "button";
      deleteBtn.className = "btn btn-danger";
      deleteBtn.textContent = t("supervisorDeleteBtn");
      deleteBtn.addEventListener("click", async () => {
        const confirmed = await openActionDialog({
          title: t("supervisorDeleteBtn"),
          message: fmt(t("supervisorDeleteConfirm"), { user: item.username }),
          confirmText: t("supervisorDeleteBtn"),
          cancelText: t("closeBtn"),
          confirmClass: "btn btn-danger"
        });
        if (!confirmed) {
          return;
        }
        try {
          await api("/api/admin/delete-user", { method: "POST", body: { username: item.username } });
          showToast(t("supervisorDeleteDone"), "success");
          refreshRoomSupervisorUsers();
        } catch (error) {
          showToast(error.message);
        }
      });
      actions.appendChild(banOrUnbanBtn);
      actions.appendChild(deleteBtn);
      card.appendChild(actions);
    }
    roomUsersList.appendChild(card);
  });
}

async function refreshRoomSupervisorAppeals() {
  if (!isSupervisor) {
    return;
  }
  try {
    const data = await api("/api/admin/ban-appeals");
    renderRoomSupervisorAppeals(data.requests || []);
  } catch (error) {
    showToast(error.message);
  }
}

async function refreshRoomSupervisorUsers() {
  if (!isSupervisor) {
    return;
  }
  try {
    const data = await api("/api/admin/users");
    renderRoomSupervisorUsers(data.users || []);
  } catch (error) {
    showToast(error.message);
  }
}

async function sendRoomSupervisorAnnouncement() {
  const text = String(roomAnnouncementInput.value || "").trim();
  if (!text) {
    showToast(t("supervisorAnnouncementNeedText"));
    return;
  }
  try {
    await api("/api/admin/site-announcement", {
      method: "POST",
      body: { text }
    });
    roomAnnouncementInput.value = "";
    showToast(t("supervisorAnnouncementDone"), "success");
  } catch (error) {
    showToast(error.message);
  }
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

function openSelfKickJokeModal() {
  if (!selfKickJokeModal) {
    return;
  }
  selfKickJokeModal.classList.remove("hidden");
}

function closeSelfKickJokeModal() {
  if (!selfKickJokeModal) {
    return;
  }
  selfKickJokeModal.classList.add("hidden");
}

function openKickSupervisorDeniedModal() {
  if (!kickSupervisorDeniedModal) {
    return;
  }
  kickSupervisorDeniedModal.classList.remove("hidden");
}

function closeKickSupervisorDeniedModal() {
  if (!kickSupervisorDeniedModal) {
    return;
  }
  kickSupervisorDeniedModal.classList.add("hidden");
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

    if (me === host) {
      const kickBtn = document.createElement("button");
      kickBtn.className = "kick-btn";
      kickBtn.type = "button";
      kickBtn.textContent = t("kickBtn");
      kickBtn.addEventListener("click", async () => {
        if (player === host) {
          sfx("click");
          openSelfKickJokeModal();
          return;
        }
        if (playerIsSupervisor) {
          sfx("click");
          openKickSupervisorDeniedModal();
          return;
        }
        try {
          await api(`/api/rooms/${encodedCode}/kick`, {
            method: "POST",
            body: { username: player }
          });
          showToast(fmt(t("toastKickedOk"), { player }), "success");
          await refreshMessages();
        } catch (error) {
          if (error.code === "SUPERVISOR_KICK_FORBIDDEN") {
            openKickSupervisorDeniedModal();
            return;
          }
          showToast(error.message);
        }
      });
      item.appendChild(kickBtn);
    }

    playersList.appendChild(item);
  });
}

function highlightMessageById(messageId) {
  const id = Number(messageId || 0);
  if (!id) {
    return;
  }
  const lineTarget = chatMessages.querySelector(`.chat-line-message[data-message-id="${id}"]`);
  const target =
    lineTarget ||
    chatMessages.querySelector(`.chat-row[data-message-id="${id}"]`) ||
    chatMessages.querySelector(`[data-message-id="${id}"]`);
  if (!target) {
    return;
  }
  target.scrollIntoView({ behavior: "smooth", block: "center" });
  const highlightClass = lineTarget ? "chat-line-highlight" : "chat-row-highlight";
  target.classList.add(highlightClass);
  setTimeout(() => {
    target.classList.remove(highlightClass);
  }, 900);
}

function createMessageLineElement(message) {
  const messageId = Number(message?.id || 0);
  const lineWrap = document.createElement("div");
  lineWrap.className = "chat-body-line-wrap chat-line-message";
  if (messageId) {
    lineWrap.dataset.messageId = String(messageId);
  }

  const lineText = document.createElement("div");
  lineText.className = "chat-body-line";
  lineText.textContent = message.text;
  lineWrap.appendChild(lineText);

  const replyBtn = document.createElement("button");
  replyBtn.type = "button";
  replyBtn.className = "chat-line-reply-btn";
  replyBtn.textContent = t("replyBtn");
  replyBtn.addEventListener("click", () => {
    setReplyTargetFromMessage(message);
  });
  lineWrap.appendChild(replyBtn);

  return lineWrap;
}

function canMergeMessageIntoLastRow(lastRow, message) {
  if (!lastRow || !message || message.type !== "user") {
    return false;
  }
  const hasReply = Boolean(message.replyTo && Number(message.replyTo.id || 0));
  if (hasReply) {
    return false;
  }
  if (lastRow.dataset.messageType !== "user") {
    return false;
  }
  if (lastRow.dataset.user !== String(message.user || "")) {
    return false;
  }
  if (lastRow.dataset.hasReply === "1") {
    return false;
  }
  return Boolean(lastRow.querySelector(".chat-body"));
}

function appendMessage(message) {
  const messageId = Number(message?.id || 0);
  const lastRow = chatMessages.lastElementChild;
  if (canMergeMessageIntoLastRow(lastRow, message)) {
    const body = lastRow.querySelector(".chat-body");
    body.appendChild(createMessageLineElement(message));
    if (messageId) {
      lastRow.dataset.messageId = String(messageId);
    }
    const time = lastRow.querySelector(".chat-time");
    if (time) {
      time.textContent = formatChatTime(message.timestamp);
    }
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return;
  }

  const row = document.createElement("div");
  const messageIsSupervisor = message.type === "user" && isSupervisorUser(message.user);
  const hasReply = Boolean(message.replyTo && Number(message.replyTo.id || 0));
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
  row.dataset.messageId = messageId ? String(messageId) : "";
  row.dataset.hasReply = hasReply ? "1" : "0";

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
  const metaTail = document.createElement("span");
  metaTail.className = "chat-meta-tail";
  metaTail.appendChild(time);
  meta.appendChild(head);
  meta.appendChild(metaTail);

  const body = document.createElement("div");
  body.className = "chat-body";
  if (message.type === "system") {
    body.textContent = systemMessageText(message);
  } else {
    if (message.replyTo && Number(message.replyTo.id || 0)) {
      const replyQuote = document.createElement("button");
      replyQuote.type = "button";
      replyQuote.className = "chat-reply-quote";
      const replyHeader = document.createElement("span");
      replyHeader.className = "chat-reply-quote-head";
      replyHeader.textContent = fmt(t("replyingTo"), {
        user: replyTargetDisplayName(message.replyTo.user)
      });
      const replyBody = document.createElement("span");
      replyBody.className = "chat-reply-quote-text";
      replyBody.textContent = shortenReplyText(message.replyTo.text);
      replyQuote.appendChild(replyHeader);
      replyQuote.appendChild(replyBody);
      replyQuote.addEventListener("click", () => {
        highlightMessageById(message.replyTo.id);
      });
      body.appendChild(replyQuote);
    }
    body.appendChild(createMessageLineElement(message));
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
  if (!room) {
    return;
  }
  host = room.host;
  roomName.textContent = room.name;
  roomCode.textContent = room.code;
  const leaderText = displayNameFor(room.host);
  roomLeader.textContent = isSupervisorUser(room.host) ? `${leaderText} (${t("supervisorBadge")})` : leaderText;
  const myText = displayNameFor(me);
  meName.textContent = isSupervisorUser(me) ? `${myText} (${t("supervisorBadge")})` : myText;
  const members = Array.isArray(room.members) ? room.members : [];
  renderRoomMembersCount(members.length);
  renderPlayers(members);
  renderRoomVideo(room);
}

async function refreshMessages() {
  if (refreshMessagesInFlight) {
    refreshMessagesQueued = true;
    return;
  }
  refreshMessagesInFlight = true;
  try {
    const result = await api(`/api/rooms/${encodedCode}/messages?since=${lastMessageId}`);
    const members = Array.isArray(result.room?.members) ? result.room.members : [];
    const messageUsers = (result.messages || []).flatMap((message) => {
      if (message.type !== "user") {
        return [];
      }
      const list = [message.user];
      if (message.replyTo?.user) {
        list.push(message.replyTo.user);
      }
      return list;
    });
    await preloadProfiles([result.room?.host, ...members, ...messageUsers, me]);
    renderRoomInfo(result.room);
    await refreshPendingRequests();
    let hasIncomingUserMessage = false;
    let latestIncomingUserMessage = null;
    result.messages.forEach((message) => {
      const messageId = Number(message?.id || 0);
      if (messageId && renderedMessageIds.has(messageId)) {
        lastMessageId = Math.max(lastMessageId, messageId);
        return;
      }
      if (message.type === "user" && message.user && message.user !== me) {
        hasIncomingUserMessage = true;
        latestIncomingUserMessage = message;
      }
      appendMessage(message);
      if (messageId) {
        renderedMessageIds.add(messageId);
        lastMessageId = Math.max(lastMessageId, messageId);
      }
    });
    if (hasIncomingUserMessage) {
      sfx("message");
      showFullscreenChatNotice(latestIncomingUserMessage);
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
  } finally {
    refreshMessagesInFlight = false;
    if (refreshMessagesQueued) {
      refreshMessagesQueued = false;
      Promise.resolve().then(() => refreshMessages());
    }
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
  sendBtn.textContent = t("sendBtn");
  videoTitle.textContent = t("videoTitle");
  videoHintText.textContent = t("videoHint");
  if (videoToolsTitle) {
    videoToolsTitle.textContent = t("videoToolsTitle");
  }
  videoFileLabel.textContent = t("videoFileLabel");
  if (roomVideoState && roomVideoState.filename) {
    videoStatusText.textContent = fmt(t("videoNowPlaying"), { name: roomVideoState.filename });
  } else {
    videoStatusText.textContent = t("videoNoVideo");
  }
  setVideoUploadBusy(isUploadingRoomVideo);
  updateRoomVideoControls();
  renderReplyDraft();
  document.getElementById("playersTitle").textContent = t("playersTitle");
  if (playersDrawerToggleText) {
    playersDrawerToggleText.textContent = t("playersDrawerBtn");
  }
  if (playersDrawerToggle) {
    playersDrawerToggle.setAttribute("aria-label", t("playersDrawerOpenLabel"));
    playersDrawerToggle.title = t("playersDrawerOpenLabel");
  }
  if (playersDrawerClose) {
    playersDrawerClose.textContent = "X";
    playersDrawerClose.setAttribute("aria-label", t("playersDrawerCloseLabel"));
    playersDrawerClose.title = t("playersDrawerCloseLabel");
  }
  if (videoToolsClose) {
    videoToolsClose.textContent = "X";
    videoToolsClose.setAttribute("aria-label", t("videoToolsCloseLabel"));
    videoToolsClose.title = t("videoToolsCloseLabel");
  }
  setPlayersDrawerOpen(playersDrawerOpen);
  setVideoToolsDrawerOpen(videoToolsOpen);
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
  roomSupervisorToggle.textContent = t("supervisorOpenBtn");
  roomSupervisorTitle.textContent = t("supervisorSidebarTitle");
  roomTabAnnouncement.textContent = t("supervisorTabAnnouncement");
  roomTabUsers.textContent = t("supervisorTabUsers");
  roomTabAppeals.textContent = t("supervisorTabAppeals");
  roomAnnouncementTitle.textContent = t("supervisorTabAnnouncement");
  roomAnnouncementDesc.textContent = t("supervisorAnnouncementDesc");
  roomAnnouncementInput.placeholder = t("supervisorAnnouncementPlaceholder");
  roomAnnouncementSend.textContent = t("supervisorAnnouncementSendBtn");
  roomUsersTitle.textContent = t("supervisorTabUsers");
  roomUsersDesc.textContent = t("supervisorUsersDesc");
  roomUsersRefresh.textContent = t("supervisorUsersRefreshBtn");
  roomAppealsTitle.textContent = t("supervisorTabAppeals");
  roomAppealsDesc.textContent = t("supervisorAppealsDesc");
  roomAppealsRefresh.textContent = t("supervisorAppealsRefreshBtn");
  roomSupervisorClose.textContent = "✕";
  pendingRequestsTitle.textContent = t("pendingRequestsTitle");
  joinModalTitle.textContent = t("joinModalTitle");
  joinModalApprove.textContent = t("approveBtn");
  joinModalReject.textContent = t("rejectBtn");
  joinModalClose.textContent = t("closeBtn");
  if (joinModalTopClose) {
    joinModalTopClose.setAttribute("aria-label", t("closeBtn"));
    joinModalTopClose.title = t("closeBtn");
  }
  if (selfKickJokeTitle) {
    selfKickJokeTitle.textContent = t("selfKickJokeTitle");
  }
  if (selfKickJokeText) {
    selfKickJokeText.textContent = t("selfKickJokeText");
  }
  if (selfKickJokeLobbyBtn) {
    selfKickJokeLobbyBtn.textContent = t("selfKickJokeLobbyBtn");
  }
  if (selfKickJokeCloseBtn) {
    selfKickJokeCloseBtn.setAttribute("aria-label", t("closeBtn"));
    selfKickJokeCloseBtn.title = t("closeBtn");
  }
  if (kickSupervisorDeniedTitle) {
    kickSupervisorDeniedTitle.textContent = t("kickSupervisorDeniedTitle");
  }
  if (kickSupervisorDeniedText) {
    kickSupervisorDeniedText.textContent = t("kickSupervisorDeniedText");
  }
  if (kickSupervisorDeniedOkBtn) {
    kickSupervisorDeniedOkBtn.textContent = t("kickSupervisorDeniedOkBtn");
  }
  if (kickSupervisorDeniedCloseBtn) {
    kickSupervisorDeniedCloseBtn.setAttribute("aria-label", t("closeBtn"));
    kickSupervisorDeniedCloseBtn.title = t("closeBtn");
  }
  if (profileModalTopClose) {
    profileModalTopClose.setAttribute("aria-label", t("closeBtn"));
    profileModalTopClose.title = t("closeBtn");
  }
  if (activeRequestUser) {
    joinModalText.textContent = fmt(t("joinModalPrompt"), { user: activeRequestUser });
    updateModalTimerText();
  }
  if (activeProfileUsername) {
    openProfileModal(activeProfileUsername);
  }
  setRoomSupervisorTab(roomSupervisorTab, false);
  renderPlayers(currentMembers);
  if (roomVideoState) {
    renderRoomVideo({ video: roomVideoState });
  }
  renderPendingRequests(currentPendingRequests);
  renderRoomSupervisorUsers(roomCachedUsers);
  renderRoomSupervisorAppeals(roomCachedAppeals);
  if (globalAnnouncementOverlay && !globalAnnouncementOverlay.classList.contains("hidden")) {
    globalAnnouncementTitle.textContent = t("announcementModalTitle");
    updateGlobalAnnouncementTimerText();
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

if (joinModalTopClose) {
  joinModalTopClose.addEventListener("click", () => {
    sfx("click");
    dismissActiveModalRequest("reject", true);
  });
}

if (joinRequestModal) {
  joinRequestModal.addEventListener("click", (event) => {
    if (event.target === joinRequestModal) {
      dismissActiveModalRequest("reject", true);
    }
  });
}

if (selfKickJokeLobbyBtn) {
  selfKickJokeLobbyBtn.addEventListener("click", async () => {
    sfx("leave");
    closeSelfKickJokeModal();
    await leaveCurrentRoom();
    window.location.href = "/";
  });
}

if (selfKickJokeCloseBtn) {
  selfKickJokeCloseBtn.addEventListener("click", () => {
    sfx("click");
    closeSelfKickJokeModal();
  });
}

if (selfKickJokeModal) {
  selfKickJokeModal.addEventListener("click", (event) => {
    if (event.target === selfKickJokeModal) {
      closeSelfKickJokeModal();
    }
  });
}

if (kickSupervisorDeniedOkBtn) {
  kickSupervisorDeniedOkBtn.addEventListener("click", () => {
    sfx("click");
    closeKickSupervisorDeniedModal();
  });
}

if (kickSupervisorDeniedCloseBtn) {
  kickSupervisorDeniedCloseBtn.addEventListener("click", () => {
    sfx("click");
    closeKickSupervisorDeniedModal();
  });
}

if (kickSupervisorDeniedModal) {
  kickSupervisorDeniedModal.addEventListener("click", (event) => {
    if (event.target === kickSupervisorDeniedModal) {
      closeKickSupervisorDeniedModal();
    }
  });
}

openMyProfileBtn.addEventListener("click", () => {
  sfx("click");
  openProfileModal(me);
});

profileCloseBtn.addEventListener("click", () => {
  sfx("click");
  closeProfileModal();
});

if (profileModalTopClose) {
  profileModalTopClose.addEventListener("click", () => {
    sfx("click");
    closeProfileModal();
  });
}

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

roomSupervisorToggle.addEventListener("click", () => {
  sfx("click");
  setRoomSupervisorOpen(!roomSupervisorOpen);
});

roomSupervisorClose.addEventListener("click", () => {
  sfx("click");
  setRoomSupervisorOpen(false);
});

roomSupervisorOverlay.addEventListener("click", () => {
  setRoomSupervisorOpen(false);
});

if (playersDrawerToggle) {
  playersDrawerToggle.addEventListener("click", () => {
    sfx("click");
    setPlayersDrawerOpen(!playersDrawerOpen);
  });
}

if (playersDrawerClose) {
  playersDrawerClose.addEventListener("click", () => {
    sfx("click");
    setPlayersDrawerOpen(false);
  });
}

if (playersDrawerOverlay) {
  playersDrawerOverlay.addEventListener("click", () => {
    setPlayersDrawerOpen(false);
  });
}

if (videoToolsToggle) {
  videoToolsToggle.addEventListener("click", () => {
    sfx("click");
    setVideoToolsDrawerOpen(!videoToolsOpen);
  });
}

if (videoToolsClose) {
  videoToolsClose.addEventListener("click", () => {
    sfx("click");
    setVideoToolsDrawerOpen(false);
  });
}

if (videoToolsOverlay) {
  videoToolsOverlay.addEventListener("click", () => {
    setVideoToolsDrawerOpen(false);
  });
}

document.addEventListener("pointerdown", (event) => {
  if ((!playersDrawerOpen && !videoToolsOpen) || !isRoomMobileLayout()) {
    return;
  }
  const target = event.target;
  if (!target) {
    return;
  }
  if (playersDrawerOpen && playersDrawer && playersDrawer.contains(target)) {
    return;
  }
  if (playersDrawerOpen && playersDrawerToggle && playersDrawerToggle.contains(target)) {
    return;
  }
  if (videoToolsOpen && videoLeaderTools && videoLeaderTools.contains(target)) {
    return;
  }
  if (videoToolsOpen && videoToolsToggle && videoToolsToggle.contains(target)) {
    return;
  }
  if (playersDrawerOpen) {
    setPlayersDrawerOpen(false);
  }
  if (videoToolsOpen) {
    setVideoToolsDrawerOpen(false);
  }
});

roomTabAnnouncement.addEventListener("click", () => {
  sfx("click");
  setRoomSupervisorTab("announcement");
});

roomTabUsers.addEventListener("click", () => {
  sfx("click");
  setRoomSupervisorTab("users");
});

roomTabAppeals.addEventListener("click", () => {
  sfx("click");
  setRoomSupervisorTab("appeals");
});

roomUsersRefresh.addEventListener("click", () => {
  sfx("click");
  refreshRoomSupervisorUsers();
});

roomAppealsRefresh.addEventListener("click", () => {
  sfx("click");
  refreshRoomSupervisorAppeals();
});

roomAnnouncementSend.addEventListener("click", () => {
  sfx("click");
  sendRoomSupervisorAnnouncement();
});

if (videoUploadBtn) {
  videoUploadBtn.addEventListener("click", () => {
    sfx("click");
    uploadRoomVideo();
  });
}

if (videoPlayerFrame) {
  const onVideoFrameInteraction = () => {
    revealRoomVideoControls();
  };
  videoPlayerFrame.addEventListener("mousemove", onVideoFrameInteraction);
  videoPlayerFrame.addEventListener("pointerdown", onVideoFrameInteraction);
  videoPlayerFrame.addEventListener("touchstart", onVideoFrameInteraction, { passive: true });
}

if (videoPlayPauseBtn) {
  videoPlayPauseBtn.addEventListener("click", () => {
    if (!roomVideoPlayer || !roomVideoState) {
      return;
    }
    sfx("click");
    revealRoomVideoControls();
    if (roomVideoPlayer.paused || roomVideoPlayer.ended) {
      roomVideoPlayer.play().catch(() => {});
      return;
    }
    roomVideoPlayer.pause();
  });
}

if (videoSeekRange) {
  videoSeekRange.addEventListener("input", () => {
    roomVideoSeekDragging = true;
    revealRoomVideoControls();
    const duration = getRoomVideoDuration();
    const progress = Number(videoSeekRange.value || 0) / 1000;
    const previewTime = duration > 0 ? progress * duration : 0;
    updateRoomVideoControls({ previewTime });
  });

  videoSeekRange.addEventListener("change", () => {
    if (!roomVideoPlayer || !roomVideoState) {
      roomVideoSeekDragging = false;
      updateRoomVideoControls();
      return;
    }
    const duration = getRoomVideoDuration();
    if (duration <= 0) {
      roomVideoSeekDragging = false;
      updateRoomVideoControls();
      return;
    }
    const progress = Number(videoSeekRange.value || 0) / 1000;
    roomVideoPlayer.currentTime = clampVideoTime(progress * duration, duration);
    roomVideoSeekDragging = false;
    scheduleRoomVideoControlsAutoHide();
    updateRoomVideoControls();
  });

  videoSeekRange.addEventListener("blur", () => {
    if (!roomVideoSeekDragging) {
      return;
    }
    roomVideoSeekDragging = false;
    scheduleRoomVideoControlsAutoHide();
    updateRoomVideoControls();
  });
}

if (videoFullscreenBtn) {
  videoFullscreenBtn.addEventListener("click", async () => {
    if (!roomVideoState) {
      return;
    }
    sfx("click");
    revealRoomVideoControls();
    try {
      if (isVideoFrameFullscreen()) {
        if (document.fullscreenElement) {
          await document.exitFullscreen();
        }
        updateRoomVideoControls();
        return;
      }
      if (videoPlayerFrame && typeof videoPlayerFrame.requestFullscreen === "function") {
        await videoPlayerFrame.requestFullscreen();
        await lockLandscapeForMobileFullscreen();
      } else if (roomVideoPlayer && typeof roomVideoPlayer.webkitEnterFullscreen === "function") {
        roomVideoPlayer.webkitEnterFullscreen();
        await lockLandscapeForMobileFullscreen();
      }
    } catch (_error) {
      // Ignore fullscreen errors.
    }
    updateRoomVideoControls();
  });
}

document.addEventListener("fullscreenchange", async () => {
  if (isVideoFrameFullscreen()) {
    await lockLandscapeForMobileFullscreen();
  } else {
    await unlockOrientationAfterFullscreen();
  }
  revealRoomVideoControls();
  updateRoomVideoControls();
});

if (roomVideoPlayer) {
  roomVideoPlayer.addEventListener("webkitbeginfullscreen", async () => {
    await lockLandscapeForMobileFullscreen();
    revealRoomVideoControls();
    updateRoomVideoControls();
  });

  roomVideoPlayer.addEventListener("webkitendfullscreen", async () => {
    await unlockOrientationAfterFullscreen();
    hideFullscreenChatNotice();
    revealRoomVideoControls();
    updateRoomVideoControls();
  });

  roomVideoPlayer.addEventListener("loadedmetadata", () => {
    roomVideoMetadataReady = true;
    revealRoomVideoControls();
    updateRoomVideoControls();
    applyRoomVideoSyncToPlayer({ forceSeek: true });
    if (isRoomLeader() && roomVideoState) {
      sendRoomVideoSync("seek");
    }
  });

  roomVideoPlayer.addEventListener("play", () => {
    scheduleRoomVideoControlsAutoHide();
    updateRoomVideoControls();
    handleRoomVideoControlEvent("play");
  });

  roomVideoPlayer.addEventListener("pause", () => {
    revealRoomVideoControls();
    updateRoomVideoControls();
    if (roomVideoPlayer.ended) {
      return;
    }
    handleRoomVideoControlEvent("pause");
  });

  roomVideoPlayer.addEventListener("seeked", () => {
    updateRoomVideoControls();
    handleRoomVideoControlEvent("seek");
  });

  roomVideoPlayer.addEventListener("ratechange", () => {
    updateRoomVideoControls();
    handleRoomVideoControlEvent("rate");
  });

  roomVideoPlayer.addEventListener("timeupdate", () => {
    if (!roomVideoSeekDragging) {
      updateRoomVideoControls();
    }
  });

  roomVideoPlayer.addEventListener("ended", () => {
    revealRoomVideoControls();
    updateRoomVideoControls();
    handleRoomVideoControlEvent("stop");
  });

  roomVideoPlayer.addEventListener("contextmenu", (event) => {
    event.preventDefault();
  });
}

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && kickSupervisorDeniedModal && !kickSupervisorDeniedModal.classList.contains("hidden")) {
    closeKickSupervisorDeniedModal();
    return;
  }
  if (event.key === "Escape" && selfKickJokeModal && !selfKickJokeModal.classList.contains("hidden")) {
    closeSelfKickJokeModal();
    return;
  }
  if (event.key === "Escape" && joinRequestModal && !joinRequestModal.classList.contains("hidden")) {
    dismissActiveModalRequest("reject", true);
    return;
  }
  if (event.key === "Escape" && profileModal && !profileModal.classList.contains("hidden")) {
    closeProfileModal();
    return;
  }
  if (event.key === "Escape" && activeReplyTarget) {
    clearReplyTarget();
    return;
  }
  if (event.key === "Escape" && playersDrawerOpen) {
    setPlayersDrawerOpen(false);
    return;
  }
  if (event.key === "Escape" && videoToolsOpen) {
    setVideoToolsDrawerOpen(false);
    return;
  }
  if (event.key === "Escape" && roomSupervisorOpen) {
    setRoomSupervisorOpen(false);
  }
});

if (roomMobileLayoutQuery && typeof roomMobileLayoutQuery.addEventListener === "function") {
  roomMobileLayoutQuery.addEventListener("change", () => {
    syncPlayersDrawerMode();
  });
}

backToLobbyLink.addEventListener("click", async (event) => {
  event.preventDefault();
  sfx("leave");
  await leaveCurrentRoom();
  window.location.href = "/";
});

if (chatReplyCancelBtn) {
  chatReplyCancelBtn.addEventListener("click", () => {
    sfx("click");
    clearReplyTarget();
  });
}

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isSendingChatMessage) {
    return;
  }
  const text = String(chatInput.value || "").trim();
  if (!text) {
    return;
  }
  const replyToMessageId = Number(activeReplyTarget?.id || 0);
  const clientMessageId = reserveClientMessageId(text, replyToMessageId);
  setChatSendingState(true);

  try {
    await api(`/api/rooms/${encodedCode}/messages`, {
      method: "POST",
      body: {
        text,
        clientMessageId,
        replyToMessageId: replyToMessageId || undefined
      }
    });
    retryableChatSend = null;
    clearReplyTarget();
    sfx("send");
    chatInput.value = "";
    await refreshMessages();
  } catch (error) {
    if (retryableChatSend) {
      retryableChatSend.timestamp = Date.now();
    }
    showToast(error.message);
  } finally {
    setChatSendingState(false);
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
    syncRoomSupervisorControls();
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
  setRoomSupervisorOpen(false);
  hideGlobalAnnouncement();
  queuedAnnouncement = null;
  closeRequestModal();
  hideFullscreenChatNotice();
  clearRoomVideoPlayer();
  unlockOrientationAfterFullscreen();
});

updateRoomVideoControls();

setLang(getLang());
syncPlayersDrawerMode();
bootRoom();


