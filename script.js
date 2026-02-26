const splash = document.getElementById("splash");
const app = document.getElementById("app");

const authSection = document.getElementById("authSection");
const lobbySection = document.getElementById("lobbySection");
const bannedSection = document.getElementById("bannedSection");
const authForm = document.getElementById("authForm");
const authBtn = document.getElementById("authBtn");
const openRegisterBtn = document.getElementById("openRegisterBtn");
const guestLoginBtn = document.getElementById("guestLoginBtn");
const logoutBtn = document.getElementById("logoutBtn");
const openProfileBtn = document.getElementById("openProfileBtn");
const currentUser = document.getElementById("currentUser");
const siteStatsText = document.getElementById("siteStatsText");
const langSelect = document.getElementById("langSelect");
const soundToggleBtn = document.getElementById("soundToggle");
const lobbyProfileModal = document.getElementById("lobbyProfileModal");
const lobbyProfileModalTopClose = document.getElementById("lobbyProfileModalTopClose");
const lobbyProfileTitle = document.getElementById("lobbyProfileTitle");
const lobbyProfileAvatar = document.getElementById("lobbyProfileAvatar");
const lobbyProfileUsername = document.getElementById("lobbyProfileUsername");
const lobbyProfileStatus = document.getElementById("lobbyProfileStatus");
const lobbyProfileCreatedAt = document.getElementById("lobbyProfileCreatedAt");
const lobbyProfileDisplayNameLabel = document.getElementById("lobbyProfileDisplayNameLabel");
const lobbyProfileDisplayNameInput = document.getElementById("lobbyProfileDisplayNameInput");
const lobbyProfileAvatarLabel = document.getElementById("lobbyProfileAvatarLabel");
const lobbyProfileAvatarInput = document.getElementById("lobbyProfileAvatarInput");
const lobbyProfileSaveBtn = document.getElementById("lobbyProfileSaveBtn");
const lobbyProfileRemoveAvatarBtn = document.getElementById("lobbyProfileRemoveAvatarBtn");
const lobbyProfileCloseBtn = document.getElementById("lobbyProfileCloseBtn");
const registerModal = document.getElementById("registerModal");
const registerModalTopClose = document.getElementById("registerModalTopClose");
const registerModalTitle = document.getElementById("registerModalTitle");
const registerForm = document.getElementById("registerForm");
const registerUsernameLabel = document.getElementById("registerUsernameLabel");
const registerUsernameInput = document.getElementById("registerUsername");
const registerPasswordLabel = document.getElementById("registerPasswordLabel");
const registerPasswordInput = document.getElementById("registerPassword");
const registerDisplayNameLabel = document.getElementById("registerDisplayNameLabel");
const registerDisplayNameInput = document.getElementById("registerDisplayName");
const registerAvatarLabel = document.getElementById("registerAvatarLabel");
const registerAvatarInput = document.getElementById("registerAvatarInput");
const registerSubmitBtn = document.getElementById("registerSubmitBtn");
const registerRemoveAvatarBtn = document.getElementById("registerRemoveAvatarBtn");
const registerCancelBtn = document.getElementById("registerCancelBtn");

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
let authMandatoryOverlay = null;
let authMandatoryTitle = null;
let authMandatoryText = null;
let authMandatoryNoticeInFlight = null;
let roomActionOverlay = null;
let roomActionTitle = null;
let roomActionText = null;
let roomActionTimer = null;
let roomActionTimerInterval = null;
let roomActionStartedAt = 0;
let roomActionMode = "";
let roomActionBusy = false;
const ROOM_ACTION_MIN_MS = 2600;
const AUTH_MANDATORY_NOTICE_MS = 5000;
let pendingJoinOverlay = null;
let pendingJoinTitle = null;
let pendingJoinText = null;
let pendingJoinCodeText = null;
let pendingJoinHint = null;
let pendingJoinCancelBtn = null;
let pendingJoinActiveCode = "";
let pendingJoinCancelInFlight = false;
let selectedLobbyAvatarDataUrl = null;
let activeLobbyProfile = null;
let selectedRegisterAvatarDataUrl = null;
const AVATAR_MAX_DATA_URL_LENGTH = 280000;
const AVATAR_DIMENSION_STEPS = [720, 600, 512, 420, 360, 300, 256];
const AVATAR_QUALITY_STEPS = [0.86, 0.78, 0.7, 0.62, 0.54, 0.46, 0.38];
let avatarWebpSupported = null;

const I18N = {
  ar: {
    pageTitle: "SawaWatch",
    langLabel: "اللغة",
    splashLabel: "مرحبًا بك في",
    splashSub: "منصة المشاهدة الجماعية",
    authTitle: "ابدأ رحلتك",
    authDesc: "سجّل الدخول إلى حسابك",
    usernameLabel: "اسم المستخدم",
    passwordLabel: "كلمة المرور",
    authBtnRegister: "إنشاء حساب",
    authBtnLogin: "دخول",
    openRegisterBtn: "إنشاء حساب جديد",
    registerModalTitle: "إنشاء حساب",
    registerDisplayNameLabel: "اسم الحساب",
    registerAvatarLabel: "الصورة الشخصية (اختياري)",
    registerCancelBtn: "إغلاق",
    guestLoginBtn: "الدخول كضيف",
    lobbyTitle: "الصفحة الرئيسية",
    welcomePrefix: "مرحبًا",
    siteStatsText: "المتواجدون الآن: {online} | داخل الغرف: {inRooms}",
    profileBtn: "الملف الشخصي",
    logoutBtn: "تسجيل الخروج",
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
    profileAvatarTooLarge: "الصورة كبيرة جدًا. اختر صورة أصغر أو أقل دقة.",
    closeBtn: "إغلاق",
    createRoomTitle: "إنشاء غرفة",
    roomNameLabel: "اسم الغرفة",
    roomNamePlaceholder: "مثال: سهرة فيلم",
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
    toastRegisterInputShort: "اسم المستخدم 3+، كلمة المرور 4+، واسم الحساب 2+ أحرف.",
    toastRegisterOk: "تم إنشاء الحساب بنجاح.",
    toastLoginOk: "تم تسجيل الدخول بنجاح.",
    toastGuestEnter: "تم الدخول كضيف.",
    authMandatoryTitle: "تنبيه",
    authMandatoryLine1: "مرحبًا بك في سوا واتش، الموقع نسخة تجريبية ومن المحتمل مواجهة مشاكل.",
    authMandatoryLine2: "شكرًا على تفهمكم.",
    toastLogoutOk: "تم تسجيل الخروج.",
    toastInvalidCode: "أدخل رمز غرفة صحيح.",
    toastJoinRequestSent: "تم إرسال طلب الانضمام.",
    toastJoinRequestCancelled: "تم إلغاء طلب الانضمام.",
    toastJoinRequestClosed: "تم إغلاق طلب الانضمام (رفض أو إزالة).",
    toastJoinApproved: "تم قبول {user}.",
    toastJoinRejected: "تم رفض طلب {user}.",
    joinWaitTitle: "طلب الانضمام قيد الانتظار",
    joinWaitText: "تم إرسال طلبك إلى قائد الغرفة: {room}",
    joinWaitCode: "رمز الغرفة: {code}",
    joinWaitHint: "انتظر قبول القائد أو رفضه. يمكنك إلغاء الطلب الآن.",
    joinWaitCancelBtn: "إلغاء طلب الانضمام",
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
    unbanTransitionSubLobby: "يمكنك المتابعة الآن. جارٍ إعادتك إلى الصفحة الرئيسية...",
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
    pageTitle: "SawaWatch",
    langLabel: "Language",
    splashLabel: "WELCOME TO",
    splashSub: "Watch Party Server",
    authTitle: "Start Your Journey",
    authDesc: "Sign in to your account",
    usernameLabel: "Username",
    passwordLabel: "Password",
    authBtnRegister: "Create Account",
    authBtnLogin: "Login",
    openRegisterBtn: "Create New Account",
    registerModalTitle: "Create Account",
    registerDisplayNameLabel: "Account Name",
    registerAvatarLabel: "Profile Image (Optional)",
    registerCancelBtn: "Close",
    guestLoginBtn: "Continue as Guest",
    lobbyTitle: "Home",
    welcomePrefix: "Welcome",
    siteStatsText: "Online now: {online} | Inside rooms: {inRooms}",
    profileBtn: "Profile",
    logoutBtn: "Logout",
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
    profileAvatarTooLarge: "Image is too large. Choose a smaller one.",
    closeBtn: "Close",
    createRoomTitle: "Create Room",
    roomNameLabel: "Room Name",
    roomNamePlaceholder: "Example: Movie Night",
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
    toastRegisterInputShort: "Username 3+, password 4+, and account name 2+ chars are required.",
    toastRegisterOk: "Account created successfully.",
    toastLoginOk: "Logged in successfully.",
    toastGuestEnter: "Entered as guest.",
    authMandatoryTitle: "Notice",
    authMandatoryLine1: "Welcome to SawaWatch. This website is an experimental version and you may encounter issues.",
    authMandatoryLine2: "Thank you for your understanding.",
    toastLogoutOk: "Logged out successfully.",
    toastInvalidCode: "Enter a valid room code.",
    toastJoinRequestSent: "Join request sent.",
    toastJoinRequestCancelled: "Join request cancelled.",
    toastJoinRequestClosed: "Join request is no longer pending (rejected or removed).",
    toastJoinApproved: "{user} was approved.",
    toastJoinRejected: "{user} was rejected.",
    joinWaitTitle: "Join Request Pending",
    joinWaitText: "Your request was sent to the room leader: {room}",
    joinWaitCode: "Room code: {code}",
    joinWaitHint: "Wait for approval or rejection. You can cancel this request now.",
    joinWaitCancelBtn: "Cancel Join Request",
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
    unbanTransitionSubLobby: "You can continue now. Returning you to the home page...",
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
  soundToggleBtn.textContent = t("soundOn");
  soundToggleBtn.classList.remove("off");
  soundToggleBtn.disabled = true;
  soundToggleBtn.setAttribute("aria-disabled", "true");
  soundToggleBtn.title = t("soundOn");
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

function normalizeCountryCode(value) {
  const code = String(value || "").trim().toUpperCase();
  return /^[A-Z]{2}$/.test(code) ? code : "";
}

function extractCountryCodeFromLocaleTag(rawTag) {
  const tag = String(rawTag || "").trim();
  if (!tag) {
    return "";
  }
  const parts = tag.split(/[-_]/g);
  if (parts.length < 2) {
    return "";
  }
  for (let i = 1; i < parts.length; i += 1) {
    const candidate = normalizeCountryCode(parts[i]);
    if (candidate) {
      return candidate;
    }
  }
  return "";
}

function detectClientCountryCode() {
  const localeCandidates = [];
  if (typeof navigator !== "undefined") {
    if (Array.isArray(navigator.languages)) {
      localeCandidates.push(...navigator.languages);
    }
    if (typeof navigator.language === "string") {
      localeCandidates.push(navigator.language);
    }
  }
  try {
    localeCandidates.push(Intl.DateTimeFormat().resolvedOptions().locale);
  } catch (_error) {
    // Ignore locale resolution failure and continue.
  }

  for (const locale of localeCandidates) {
    const fromLocale = extractCountryCodeFromLocaleTag(locale);
    if (fromLocale) {
      return fromLocale;
    }
  }
  return "";
}

const CLIENT_COUNTRY_CODE = detectClientCountryCode();

function countryCodeToFlagEmoji(countryCode) {
  const code = normalizeCountryCode(countryCode);
  if (!code) {
    return "";
  }
  return String.fromCodePoint(...Array.from(code).map((ch) => ch.charCodeAt(0) + 127397));
}

function countryCodeToFlagIconUrl(countryCode) {
  const code = normalizeCountryCode(countryCode);
  if (!code) {
    return "";
  }
  return `https://flagcdn.com/24x18/${code.toLowerCase()}.png`;
}

function createCountryFlagBadge(countryCode) {
  const code = normalizeCountryCode(countryCode);
  if (!code) {
    return null;
  }
  const badge = document.createElement("span");
  badge.className = "country-flag-badge";
  badge.title = code;
  badge.setAttribute("aria-label", code);
  const img = document.createElement("img");
  img.src = countryCodeToFlagIconUrl(code);
  img.alt = code;
  img.width = 24;
  img.height = 18;
  img.decoding = "async";
  img.referrerPolicy = "no-referrer";
  img.addEventListener(
    "error",
    () => {
      const fallback = countryCodeToFlagEmoji(code);
      badge.classList.add("is-fallback-text");
      badge.textContent = fallback || code;
    },
    { once: true }
  );
  badge.appendChild(img);
  return badge;
}

function closeLobbyProfileModal() {
  activeLobbyProfile = null;
  selectedLobbyAvatarDataUrl = null;
  if (lobbyProfileAvatarInput) {
    lobbyProfileAvatarInput.value = "";
  }
  if (lobbyProfileModal) {
    lobbyProfileModal.classList.remove("is-supervisor-profile");
    lobbyProfileModal.classList.add("hidden");
  }
}

function supportsWebpEncoding() {
  if (avatarWebpSupported !== null) {
    return avatarWebpSupported;
  }
  try {
    const canvas = document.createElement("canvas");
    avatarWebpSupported = canvas.toDataURL("image/webp", 0.8).startsWith("data:image/webp");
  } catch (_error) {
    avatarWebpSupported = false;
  }
  return avatarWebpSupported;
}

function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const objectUrl = URL.createObjectURL(file);
    const image = new Image();
    image.onload = () => {
      URL.revokeObjectURL(objectUrl);
      resolve(image);
    };
    image.onerror = () => {
      URL.revokeObjectURL(objectUrl);
      reject(new Error(t("profileAvatarTooLarge")));
    };
    image.src = objectUrl;
  });
}

function drawAvatarFrame(ctx, image, width, height, mimeType) {
  ctx.clearRect(0, 0, width, height);
  if (mimeType === "image/jpeg") {
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, width, height);
  }
  ctx.drawImage(image, 0, 0, width, height);
}

async function readFileAsDataUrl(file) {
  if (!file || !String(file.type || "").startsWith("image/")) {
    throw new Error(t("profileAvatarTooLarge"));
  }
  const image = await loadImageFromFile(file);
  const sourceWidth = Math.max(1, Number(image.naturalWidth || image.width || 0));
  const sourceHeight = Math.max(1, Number(image.naturalHeight || image.height || 0));
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d", { alpha: false });
  if (!ctx) {
    throw new Error(t("profileAvatarTooLarge"));
  }
  const mimeType = supportsWebpEncoding() ? "image/webp" : "image/jpeg";
  for (const maxDimension of AVATAR_DIMENSION_STEPS) {
    const scale = Math.min(1, maxDimension / Math.max(sourceWidth, sourceHeight));
    const width = Math.max(1, Math.round(sourceWidth * scale));
    const height = Math.max(1, Math.round(sourceHeight * scale));
    canvas.width = width;
    canvas.height = height;
    drawAvatarFrame(ctx, image, width, height, mimeType);
    for (const quality of AVATAR_QUALITY_STEPS) {
      let dataUrl = "";
      try {
        dataUrl = canvas.toDataURL(mimeType, quality);
      } catch (_error) {
        dataUrl = "";
      }
      if (dataUrl && dataUrl.length <= AVATAR_MAX_DATA_URL_LENGTH) {
        return dataUrl;
      }
    }
  }
  throw new Error(t("profileAvatarTooLarge"));
}

async function openLobbyProfileModal() {
  if (!lobbyProfileModal) {
    return;
  }
  try {
    const result = await api("/api/profile");
    const profile = result?.profile;
    if (!profile) {
      showToast(t("profileOpenFail"));
      return;
    }
    activeLobbyProfile = profile;
    selectedLobbyAvatarDataUrl = profile.avatarDataUrl || null;
    lobbyProfileTitle.textContent = t("profileTitle");
    lobbyProfileUsername.textContent = "";
    const identity = document.createElement("span");
    identity.textContent = `@${profile.username} • ${profile.displayName || profile.username}`;
    lobbyProfileUsername.appendChild(identity);
    const countryFlagBadge = createCountryFlagBadge(profile.countryCode || CLIENT_COUNTRY_CODE);
    if (countryFlagBadge) {
      lobbyProfileUsername.appendChild(countryFlagBadge);
    }
    lobbyProfileStatus.textContent = profile.isOnline ? t("profileOnline") : t("profileOffline");
    lobbyProfileCreatedAt.textContent = fmt(t("profileCreated"), { date: formatDate(profile.createdAt) });
    lobbyProfileDisplayNameInput.value = profile.displayName || profile.username;
    if (profile.avatarDataUrl) {
      lobbyProfileAvatar.src = profile.avatarDataUrl;
    } else {
      lobbyProfileAvatar.removeAttribute("src");
    }
    lobbyProfileModal.classList.toggle("is-supervisor-profile", Boolean(profile.isSupervisor));
    lobbyProfileModal.classList.remove("hidden");
    sfx("modal");
  } catch (error) {
    showToast(error.message || t("profileOpenFail"));
  }
}

async function saveLobbyProfile() {
  if (!activeLobbyProfile) {
    return;
  }
  const displayName = String(lobbyProfileDisplayNameInput.value || "").trim();
  if (displayName.length < 2 || displayName.length > 30) {
    showToast(t("profileNameInvalid"));
    return;
  }
  const body = { displayName };
  if (selectedLobbyAvatarDataUrl !== null) {
    body.avatarDataUrl = selectedLobbyAvatarDataUrl;
  }
  await api("/api/profile", { method: "PATCH", body });
  showToast(t("profileSaved"), "success");
  await openLobbyProfileModal();
}

function closeRegisterModal() {
  selectedRegisterAvatarDataUrl = null;
  if (registerForm) {
    registerForm.reset();
  }
  if (registerModal) {
    registerModal.classList.add("hidden");
  }
}

function openRegisterModal() {
  if (!registerModal) {
    return;
  }
  selectedRegisterAvatarDataUrl = null;
  if (registerForm) {
    registerForm.reset();
  }
  registerModal.classList.remove("hidden");
  sfx("modal");
}

async function createAccountFromModal() {
  const username = String(registerUsernameInput?.value || "").trim();
  const password = String(registerPasswordInput?.value || "");
  const displayName = String(registerDisplayNameInput?.value || "").trim();
  if (username.length < 3 || password.length < 4 || displayName.length < 2) {
    showToast(t("toastRegisterInputShort"));
    return;
  }

  const result = await api("/api/register", {
    method: "POST",
    body: { username, password }
  });
  setSession(result.token, result.username);

  const profilePayload = { displayName };
  if (selectedRegisterAvatarDataUrl !== null) {
    profilePayload.avatarDataUrl = selectedRegisterAvatarDataUrl;
  }
  await api("/api/profile", { method: "PATCH", body: profilePayload });

  try {
    const meData = await api("/api/me");
    isSupervisor = Boolean(meData.isSupervisor);
  } catch (_) {
    isSupervisor = false;
  }
  await showAuthMandatoryNotice();
  closeRegisterModal();
  showToast(t("toastRegisterOk"), "success");
  showLobby();
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
  closeLobbyProfileModal();
  closeRegisterModal();
  hidePendingJoinOverlay();
  hideGlobalAnnouncement();
  hideAuthMandatoryNotice();
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

function ensureAuthMandatoryOverlay() {
  if (authMandatoryOverlay) {
    return authMandatoryOverlay;
  }
  authMandatoryOverlay = document.createElement("div");
  authMandatoryOverlay.className = "modal-overlay forced-announcement-overlay auth-mandatory-overlay hidden";
  authMandatoryOverlay.innerHTML = `
    <div class="modal-card forced-announcement-card auth-mandatory-card" role="dialog" aria-modal="true">
      <h3 id="authMandatoryTitle"></h3>
      <p id="authMandatoryText" class="forced-announcement-text auth-mandatory-text"></p>
    </div>
  `;
  authMandatoryOverlay.addEventListener("click", (event) => {
    if (event.target === authMandatoryOverlay) {
      event.preventDefault();
    }
  });
  document.body.appendChild(authMandatoryOverlay);
  authMandatoryTitle = authMandatoryOverlay.querySelector("#authMandatoryTitle");
  authMandatoryText = authMandatoryOverlay.querySelector("#authMandatoryText");
  return authMandatoryOverlay;
}

function hideAuthMandatoryNotice() {
  if (authMandatoryOverlay) {
    authMandatoryOverlay.classList.add("hidden");
  }
  document.body.classList.remove("auth-mandatory-lock");
}

async function showAuthMandatoryNotice() {
  if (authMandatoryNoticeInFlight) {
    return authMandatoryNoticeInFlight;
  }
  authMandatoryNoticeInFlight = (async () => {
    const overlay = ensureAuthMandatoryOverlay();
    if (authMandatoryTitle) {
      authMandatoryTitle.textContent = t("authMandatoryTitle");
    }
    if (authMandatoryText) {
      authMandatoryText.textContent = `${t("authMandatoryLine1")}\n${t("authMandatoryLine2")}`;
    }
    overlay.classList.remove("hidden");
    document.body.classList.add("auth-mandatory-lock");
    sfx("notify");
    await delay(AUTH_MANDATORY_NOTICE_MS);
    hideAuthMandatoryNotice();
  })()
    .catch(() => {
      hideAuthMandatoryNotice();
    })
    .finally(() => {
      authMandatoryNoticeInFlight = null;
    });
  return authMandatoryNoticeInFlight;
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

function ensurePendingJoinOverlay() {
  if (pendingJoinOverlay) {
    return pendingJoinOverlay;
  }
  pendingJoinOverlay = document.createElement("div");
  pendingJoinOverlay.className = "modal-overlay join-request-wait-overlay hidden";
  pendingJoinOverlay.innerHTML = `
    <div class="modal-card join-request-wait-card" role="dialog" aria-modal="true">
      <div class="operation-loader join-request-wait-loader" aria-hidden="true"></div>
      <h3 id="pendingJoinTitle"></h3>
      <p id="pendingJoinText" class="muted"></p>
      <p id="pendingJoinCode" class="join-request-wait-code"></p>
      <p id="pendingJoinHint" class="muted join-request-wait-hint"></p>
      <div class="room-actions">
        <button id="pendingJoinCancelBtn" class="btn btn-ghost" type="button"></button>
      </div>
    </div>
  `;
  document.body.appendChild(pendingJoinOverlay);
  pendingJoinTitle = pendingJoinOverlay.querySelector("#pendingJoinTitle");
  pendingJoinText = pendingJoinOverlay.querySelector("#pendingJoinText");
  pendingJoinCodeText = pendingJoinOverlay.querySelector("#pendingJoinCode");
  pendingJoinHint = pendingJoinOverlay.querySelector("#pendingJoinHint");
  pendingJoinCancelBtn = pendingJoinOverlay.querySelector("#pendingJoinCancelBtn");

  if (pendingJoinCancelBtn) {
    pendingJoinCancelBtn.addEventListener("click", async () => {
      sfx("click");
      await cancelPendingJoinRequest();
    });
  }

  // This modal is intentionally forced; outside clicks do not close it.
  pendingJoinOverlay.addEventListener("click", (event) => {
    if (event.target === pendingJoinOverlay) {
      event.preventDefault();
    }
  });
  return pendingJoinOverlay;
}

function hidePendingJoinOverlay() {
  if (pendingJoinOverlay) {
    pendingJoinOverlay.classList.add("hidden");
  }
  pendingJoinActiveCode = "";
  pendingJoinCancelInFlight = false;
  if (pendingJoinCancelBtn) {
    pendingJoinCancelBtn.disabled = false;
    pendingJoinCancelBtn.textContent = t("joinWaitCancelBtn");
  }
  document.body.classList.remove("pending-join-lock");
}

function updatePendingJoinOverlayText(room = null, code = "") {
  if (!pendingJoinOverlay) {
    return;
  }
  const safeCode = String(code || pendingJoinActiveCode || "").trim().toUpperCase();
  const roomLabel = String(room?.name || safeCode || "-");
  if (pendingJoinTitle) {
    pendingJoinTitle.textContent = t("joinWaitTitle");
  }
  if (pendingJoinText) {
    pendingJoinText.textContent = fmt(t("joinWaitText"), { room: roomLabel });
  }
  if (pendingJoinCodeText) {
    pendingJoinCodeText.textContent = fmt(t("joinWaitCode"), { code: safeCode || "-" });
  }
  if (pendingJoinHint) {
    pendingJoinHint.textContent = t("joinWaitHint");
  }
  if (pendingJoinCancelBtn) {
    pendingJoinCancelBtn.textContent = t("joinWaitCancelBtn");
  }
}

function showPendingJoinOverlay(room = null, code = "") {
  const overlay = ensurePendingJoinOverlay();
  pendingJoinActiveCode = String(code || pendingJoinActiveCode || room?.code || "").trim().toUpperCase();
  updatePendingJoinOverlayText(room, pendingJoinActiveCode);
  overlay.classList.remove("hidden");
  document.body.classList.add("pending-join-lock");
}

async function cancelPendingJoinRequest(code = "") {
  const targetCode = String(code || pendingJoinActiveCode || localStorage.getItem(PENDING_JOIN_KEY) || "")
    .trim()
    .toUpperCase();
  if (!targetCode || pendingJoinCancelInFlight) {
    return;
  }
  pendingJoinCancelInFlight = true;
  if (pendingJoinCancelBtn) {
    pendingJoinCancelBtn.disabled = true;
  }
  try {
    await api(`/api/rooms/${encodeURIComponent(targetCode)}/request-join`, {
      method: "POST",
      body: { action: "cancel" }
    });
    localStorage.removeItem(PENDING_JOIN_KEY);
    hidePendingJoinOverlay();
    showToast(t("toastJoinRequestCancelled"), "success");
    await refreshPublicRooms();
  } catch (error) {
    showToast(error.message);
  } finally {
    pendingJoinCancelInFlight = false;
    if (pendingJoinCancelBtn) {
      pendingJoinCancelBtn.disabled = false;
      pendingJoinCancelBtn.textContent = t("joinWaitCancelBtn");
    }
  }
}

async function api(pathname, options = {}) {
  const token = getToken();
  const headers = {
    "Content-Type": "application/json",
    "X-Lang": getLang(),
    ...(CLIENT_COUNTRY_CODE ? { "X-Country": CLIENT_COUNTRY_CODE } : {}),
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

function setLobbyBackgroundAnimation(active) {
  void active;
}

function showAuth() {
  setLobbyBackgroundAnimation(false);
  stopLobbyPolling();
  stopBanStatusPolling();
  closeLobbyProfileModal();
  closeRegisterModal();
  hidePendingJoinOverlay();
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
  setLobbyBackgroundAnimation(true);
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
  setLobbyBackgroundAnimation(false);
  stopLobbyPolling();
  stopBanStatusPolling();
  hidePendingJoinOverlay();
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
    hidePendingJoinOverlay();
    return false;
  }
  const room = rooms.find((item) => item.code === pendingCode);
  if (!room) {
    localStorage.removeItem(PENDING_JOIN_KEY);
    hidePendingJoinOverlay();
    showToast(t("toastJoinRequestClosed"));
    return false;
  }
  if (room.isMember) {
    localStorage.removeItem(PENDING_JOIN_KEY);
    hidePendingJoinOverlay();
    await enterRoomWithOverlay(room.code);
    return true;
  }
  if (room.hasPendingRequest) {
    showPendingJoinOverlay(room, pendingCode);
    return false;
  }
  if (!room.hasPendingRequest) {
    localStorage.removeItem(PENDING_JOIN_KEY);
    hidePendingJoinOverlay();
    showToast(t("toastJoinRequestClosed"));
  }
  return false;
}

async function handleJoinRequest(code) {
  try {
    const result = await api(`/api/rooms/${encodeURIComponent(code)}/request-join`, { method: "POST" });
    if (result.status === "already_member" && result.room?.code) {
      localStorage.removeItem(PENDING_JOIN_KEY);
      hidePendingJoinOverlay();
      await enterRoomWithOverlay(result.room.code);
      return;
    }
    localStorage.setItem(PENDING_JOIN_KEY, code);
    showPendingJoinOverlay({ code, name: code }, code);
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
  document.getElementById("usernameLabel").textContent = t("usernameLabel");
  document.getElementById("passwordLabel").textContent = t("passwordLabel");
  if (authBtn) {
    authBtn.textContent = t("authBtnLogin");
  }
  if (openRegisterBtn) {
    openRegisterBtn.textContent = t("openRegisterBtn");
  }
  if (registerModalTitle) {
    registerModalTitle.textContent = t("registerModalTitle");
  }
  if (registerUsernameLabel) {
    registerUsernameLabel.textContent = t("usernameLabel");
  }
  if (registerPasswordLabel) {
    registerPasswordLabel.textContent = t("passwordLabel");
  }
  if (registerDisplayNameLabel) {
    registerDisplayNameLabel.textContent = t("registerDisplayNameLabel");
  }
  if (registerAvatarLabel) {
    registerAvatarLabel.textContent = t("registerAvatarLabel");
  }
  if (registerSubmitBtn) {
    registerSubmitBtn.textContent = t("authBtnRegister");
  }
  if (registerRemoveAvatarBtn) {
    registerRemoveAvatarBtn.textContent = t("profileRemoveAvatar");
  }
  if (registerCancelBtn) {
    registerCancelBtn.textContent = t("registerCancelBtn");
  }
  if (registerModalTopClose) {
    registerModalTopClose.setAttribute("aria-label", t("closeBtn"));
    registerModalTopClose.title = t("closeBtn");
  }
  if (guestLoginBtn) {
    guestLoginBtn.textContent = t("guestLoginBtn");
  }
  document.getElementById("lobbyTitle").textContent = t("lobbyTitle");
  document.getElementById("welcomePrefix").textContent = t("welcomePrefix");
  renderSiteStats(cachedSiteStats);
  if (openProfileBtn) {
    openProfileBtn.textContent = t("profileBtn");
  }
  document.getElementById("logoutBtn").textContent = t("logoutBtn");
  if (lobbyProfileTitle) {
    lobbyProfileTitle.textContent = t("profileTitle");
  }
  if (lobbyProfileDisplayNameLabel) {
    lobbyProfileDisplayNameLabel.textContent = t("profileDisplayName");
  }
  if (lobbyProfileAvatarLabel) {
    lobbyProfileAvatarLabel.textContent = t("profileAvatar");
  }
  if (lobbyProfileSaveBtn) {
    lobbyProfileSaveBtn.textContent = t("profileSave");
  }
  if (lobbyProfileRemoveAvatarBtn) {
    lobbyProfileRemoveAvatarBtn.textContent = t("profileRemoveAvatar");
  }
  if (lobbyProfileCloseBtn) {
    lobbyProfileCloseBtn.textContent = t("closeBtn");
  }
  if (lobbyProfileModalTopClose) {
    lobbyProfileModalTopClose.setAttribute("aria-label", t("closeBtn"));
    lobbyProfileModalTopClose.title = t("closeBtn");
  }
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
  if (pendingJoinOverlay && !pendingJoinOverlay.classList.contains("hidden")) {
    const pendingCode = String(localStorage.getItem(PENDING_JOIN_KEY) || "").trim().toUpperCase();
    const pendingRoom = cachedRooms.find((item) => item.code === pendingCode) || null;
    updatePendingJoinOverlayText(pendingRoom, pendingCode);
  } else if (pendingJoinCancelBtn) {
    pendingJoinCancelBtn.textContent = t("joinWaitCancelBtn");
  }
  if (lobbyProfileModal && !lobbyProfileModal.classList.contains("hidden")) {
    openLobbyProfileModal().catch(() => {});
  }
}

langSelect.addEventListener("change", (event) => {
  sfx("click");
  setLang(event.target.value);
});

if (openRegisterBtn) {
  openRegisterBtn.addEventListener("click", () => {
    sfx("click");
    window.location.href = "register.html";
  });
}

if (registerCancelBtn) {
  registerCancelBtn.addEventListener("click", () => {
    sfx("click");
    closeRegisterModal();
  });
}

if (registerModalTopClose) {
  registerModalTopClose.addEventListener("click", () => {
    sfx("click");
    closeRegisterModal();
  });
}

if (registerModal) {
  registerModal.addEventListener("click", (event) => {
    if (event.target === registerModal) {
      closeRegisterModal();
    }
  });
}

if (registerAvatarInput) {
  registerAvatarInput.addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    try {
      const dataUrl = await readFileAsDataUrl(file);
      selectedRegisterAvatarDataUrl = dataUrl;
    } catch (error) {
      if (registerAvatarInput) {
        registerAvatarInput.value = "";
      }
      showToast(error.message);
    }
  });
}

if (registerRemoveAvatarBtn) {
  registerRemoveAvatarBtn.addEventListener("click", () => {
    sfx("click");
    selectedRegisterAvatarDataUrl = "";
    if (registerAvatarInput) {
      registerAvatarInput.value = "";
    }
  });
}

if (registerForm) {
  registerForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await createAccountFromModal();
    } catch (error) {
      if (error.code === "ACCOUNT_BANNED") {
        sfx("ban");
        const rawUser = String(registerUsernameInput?.value || "").trim();
        localStorage.setItem(USER_KEY, rawUser.toLowerCase());
        await transitionToBanned(error.data?.ban, rawUser);
        return;
      }
      showToast(error.message);
    }
  });
}

if (guestLoginBtn) {
  guestLoginBtn.addEventListener("click", () => {
    sfx("click");
    window.location.href = "guest.html";
  });
}

if (openProfileBtn) {
  openProfileBtn.addEventListener("click", () => {
    sfx("click");
    openLobbyProfileModal();
  });
}

if (lobbyProfileCloseBtn) {
  lobbyProfileCloseBtn.addEventListener("click", () => {
    sfx("click");
    closeLobbyProfileModal();
  });
}

if (lobbyProfileModalTopClose) {
  lobbyProfileModalTopClose.addEventListener("click", () => {
    sfx("click");
    closeLobbyProfileModal();
  });
}

if (lobbyProfileModal) {
  lobbyProfileModal.addEventListener("click", (event) => {
    if (event.target === lobbyProfileModal) {
      closeLobbyProfileModal();
    }
  });
}

if (lobbyProfileAvatarInput) {
  lobbyProfileAvatarInput.addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    try {
      const dataUrl = await readFileAsDataUrl(file);
      selectedLobbyAvatarDataUrl = dataUrl;
      lobbyProfileAvatar.src = dataUrl;
    } catch (error) {
      if (lobbyProfileAvatarInput) {
        lobbyProfileAvatarInput.value = "";
      }
      showToast(error.message);
    }
  });
}

if (lobbyProfileRemoveAvatarBtn) {
  lobbyProfileRemoveAvatarBtn.addEventListener("click", () => {
    sfx("click");
    selectedLobbyAvatarDataUrl = "";
    lobbyProfileAvatar.removeAttribute("src");
    if (lobbyProfileAvatarInput) {
      lobbyProfileAvatarInput.value = "";
    }
  });
}

if (lobbyProfileSaveBtn) {
  lobbyProfileSaveBtn.addEventListener("click", async () => {
    try {
      await saveLobbyProfile();
    } catch (error) {
      showToast(error.message);
    }
  });
}

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
    const result = await api("/api/login", {
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
    await showAuthMandatoryNotice();
    showToast(t("toastLoginOk"), "success");
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
  if (event.key === "Escape" && registerModal && !registerModal.classList.contains("hidden")) {
    closeRegisterModal();
    return;
  }
  if (event.key === "Escape" && pendingJoinOverlay && !pendingJoinOverlay.classList.contains("hidden")) {
    event.preventDefault();
    return;
  }
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


