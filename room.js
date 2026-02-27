const TOKEN_KEY = "bedna_token";
const USER_KEY = "bedna_user";
const LANG_KEY = "bedna_lang";
const ANNOUNCEMENT_SEEN_KEY = "bedna_seen_announcement_id";
const ROOM_VIDEO_VOLUME_KEY = "bedna_room_video_volume";

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
const quickLeaveBtn = document.getElementById("quickLeaveBtn");
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
const chatComposerOpenBtn = document.getElementById("chatComposerOpenBtn");
const chatComposerCloseBtn = document.getElementById("chatComposerCloseBtn");
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
const roomVideoEmbed = document.getElementById("roomVideoEmbed");
const videoYouTubeMask = document.getElementById("videoYouTubeMask");
const videoEmptyNotice = document.getElementById("videoEmptyNotice");
const videoEmptyNoticeLine1 = document.getElementById("videoEmptyNoticeLine1");
const videoEmptyNoticeLine2 = document.getElementById("videoEmptyNoticeLine2");
const videoQueueCountdownNotice = document.getElementById("videoQueueCountdownNotice");
const videoControlsBar = document.getElementById("videoControlsBar");
const videoPlayPauseBtn = document.getElementById("videoPlayPauseBtn");
const videoVolumeWrap = document.getElementById("videoVolumeWrap");
const videoVolumeBtn = document.getElementById("videoVolumeBtn");
const videoVolumeRange = document.getElementById("videoVolumeRange");
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
const videoYoutubeLabel = document.getElementById("videoYoutubeLabel");
const videoYoutubeInput = document.getElementById("videoYoutubeInput");
const videoYoutubeBtn = document.getElementById("videoYoutubeBtn");
const videoClearBtn = document.getElementById("videoClearBtn");
const videoClearTopBtn = document.getElementById("videoClearTopBtn");
const videoQueueTools = document.getElementById("videoQueueTools");
const videoQueueTitle = document.getElementById("videoQueueTitle");
const videoQueuePrevBtn = document.getElementById("videoQueuePrevBtn");
const videoQueueNextBtn = document.getElementById("videoQueueNextBtn");
const videoQueueEmpty = document.getElementById("videoQueueEmpty");
const videoQueueList = document.getElementById("videoQueueList");

const query = new URLSearchParams(window.location.search);
const code = String(query.get("code") || "").trim().toUpperCase();
const encodedCode = encodeURIComponent(code);
const roomMobileLayoutQuery = window.matchMedia("(max-width: 980px)");
const videoToolsOriginalParent = videoLeaderTools ? videoLeaderTools.parentNode : null;
const videoToolsOriginalNextSibling = videoLeaderTools ? videoLeaderTools.nextSibling : null;
const videoToolsOverlayOriginalParent = videoToolsOverlay ? videoToolsOverlay.parentNode : null;
const videoToolsOverlayOriginalNextSibling = videoToolsOverlay ? videoToolsOverlay.nextSibling : null;

document.body.classList.add("room-page");
document.documentElement.classList.add("room-page");

let me = "";
let host = "";
let lastMessageId = 0;
let pollTimer = null;
let roomSocket = null;
let roomSocketConnected = false;
let roomSocketReconnectTimer = null;
let roomSocketReconnectAttempt = 0;
let roomSocketManualClose = false;
let roomRefreshDebounceTimer = null;
let currentPendingRequests = [];
let requestModalQueue = [];
let activeRequestUser = "";
let requestModalSeconds = 0;
let requestModalTimer = null;
const dismissedRequestUsers = new Set();
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
const AVATAR_MAX_DATA_URL_LENGTH = 280000;
const AVATAR_DIMENSION_STEPS = [720, 600, 512, 420, 360, 300, 256];
const AVATAR_QUALITY_STEPS = [0.86, 0.78, 0.7, 0.62, 0.54, 0.46, 0.38];
let avatarWebpSupported = null;
let retryableChatSend = null;
let chatLastOutsidePointerAt = 0;
let mobileChatComposerOpen = false;
let mobileChatKeyboardOffsetPx = 0;
let mobileChatViewportEventsBound = false;
let suppressNextMobileSendButtonClickUntil = 0;
let activeReplyTarget = null;
const renderedMessageIds = new Set();
const CHAT_SEND_RETRY_WINDOW_MS = 30000;
const CHAT_REACTION_CHOICES = Object.freeze(["👍", "❤️", "😂", "🔥", "😮", "😢"]);
const MOBILE_CHAT_KEYBOARD_OPEN_THRESHOLD_PX = 72;
const ROOM_VIDEO_MAX_BYTES = 1024 * 1024 * 1024;
const ROOM_VIDEO_SYNC_LOCK_TOAST_MS = 4000;
const ROOM_VIDEO_LEADER_SEEK_DRIFT_SEC = 0.9;
const ROOM_VIDEO_MEMBER_HARD_SEEK_DRIFT_SEC = 1.6;
const ROOM_VIDEO_MEMBER_SOFT_DRIFT_SEC = 0.35;
const ROOM_VIDEO_MEMBER_CATCHUP_FACTOR = 0.08;
const ROOM_VIDEO_MEMBER_CATCHUP_LIMIT = 0.18;
const ROOM_VIDEO_SYNC_HEARTBEAT_MS = 1200;
const ROOM_VIDEO_LEADER_SYNC_SUPPRESS_MS = 1400;
const ROOM_VIDEO_SYNC_STALE_REWIND_SEC = 0.55;
const ROOM_VIDEO_SYNC_STALE_PAUSE_REWIND_SEC = 0.2;
const ROOM_VIDEO_CONTROLS_HIDE_DELAY_MS = 2200;
const ROOM_VIDEO_CONTROLS_AUTO_HIDE = true;
const ROOM_VIDEO_EVENT_SUPPRESS_MS = 160;
const ROOM_VIDEO_AUTO_NEXT_DELAY_SEC = 5;
const ROOM_VIDEO_SOURCE_SWITCH_GUARD_MS = 2400;
const ROOM_VIDEO_REWIND_ALLOW_MS = 2400;
const ROOM_VIDEO_SEEK_RANGE_MAX = 10000;
const ROOM_VIDEO_DEFAULT_VOLUME = 1;
const FULLSCREEN_CHAT_NOTICE_TIMEOUT_MS = 2600;
const ROOM_POLL_CONNECTED_MS = 12000;
const ROOM_POLL_DISCONNECTED_MS = 3500;
const ROOM_WS_RECONNECT_BASE_MS = 800;
const ROOM_WS_RECONNECT_MAX_MS = 8000;
const ROOM_WS_REFRESH_DEBOUNCE_MS = 120;
let roomVideoState = null;
let roomVideoSyncState = null;
let roomVideoQueueState = [];
let activeRoomVideoId = "";
let roomVideoMetadataReady = false;
let suppressRoomVideoEvents = false;
let isUploadingRoomVideo = false;
let isManagingRoomVideoQueue = false;
let roomVideoSyncToastUntil = 0;
let roomVideoSeekDragging = false;
let roomVideoControlsHideTimer = null;
let roomVideoProgressAnimFrame = 0;
let fullscreenChatNoticeEl = null;
let fullscreenChatNoticeTimer = null;
let youTubeApiReadyPromise = null;
let youTubePlayer = null;
let youTubePlayerReady = false;
let youTubeState = -1;
let youTubeTickTimer = null;
let activeYouTubeVideoId = "";
let suppressYouTubeStateBroadcast = false;
let youTubeAudioUnlockNeeded = false;
let youTubeInitialSyncPending = false;
let youTubeLeaderPlayPending = false;
let roomVideoUserInteracted = false;
let videoToolsModalHideTimer = null;
let roomVideoSyncHeartbeatTimer = null;
let roomVideoLeaderIgnoreSyncUntil = 0;
let roomVideoSyncClientSeq = 0;
let roomVideoLastKnownTime = 0;
let roomVideoSourceChangedAt = 0;
let roomVideoAllowRewindUntil = 0;
let roomVideoPageLeaving = false;
let roomVideoResumePending = false;
let roomVideoEndTransitionInFlight = false;
let roomVideoAutoNextCountdownTimer = null;
let roomVideoAutoNextCountdownSeconds = 0;
let roomVideoAutoNextVideoId = "";
let roomVideoLocalVolume = ROOM_VIDEO_DEFAULT_VOLUME;
let roomVideoLocalMuted = false;
let roomVideoVolumeBeforeMute = ROOM_VIDEO_DEFAULT_VOLUME;
let roomVideoUploadOverlay = null;
let roomVideoUploadTitleEl = null;
let roomVideoUploadHintEl = null;
let roomVideoUploadFileLabelEl = null;
let roomVideoUploadFileValueEl = null;
let roomVideoUploadProgressLabelEl = null;
let roomVideoUploadProgressValueEl = null;
let roomVideoUploadPercentLabelEl = null;
let roomVideoUploadPercentValueEl = null;
let roomVideoUploadStatusEl = null;
let roomVideoUploadBarFillEl = null;
let roomVideoUploadHideTimer = null;
let activeReactionPickerMessageId = 0;
const messageReactionState = new Map();
const reactionToggleInFlight = new Set();
let roomVideoUploadState = {
  fileName: "",
  totalBytes: 0,
  loadedBytes: 0,
  percent: 0,
  status: "idle"
};

const I18N = {
  ar: {
    pageTitle: "SawaWatch - الغرفة",
    langLabel: "اللغة",
    codeLabel: "الرمز",
    leaderLabel: "القائد",
    youLabel: "أنت",
    backToLobby: "خروج من الغرفة",
    quickLeave: "خروج",
    chatTitle: "دردشة الغرفة",
    chatPlaceholder: "اكتب رسالتك...",
    sendBtn: "إرسال",
    chatComposerOpen: "فتح الكتابة",
    chatComposerClose: "إغلاق الكتابة",
    videoTitle: "فيديو الغرفة",
    videoNoVideo: "لا يوجد فيديو مرفوع بعد.",
    videoNowPlaying: "الفيديو الحالي: {name}",
    videoNowPlayingWithUser: "الفيديو الحالي: {name} • بواسطة {user}",
    videoByUserInline: "• بواسطة {user}",
    videoHint: "يمكن لكل الأعضاء إضافة فيديوهات، والتحكم بالمزامنة بيد قائد الغرفة.",
    videoEmptyLine1: "تم إنشاء هذا الموقع لتسهيل مشاهدة الأفلام والفيديوهات.",
    videoEmptyLine2: "صُمم هذا الموقع لتجربة مشاهدة جماعية مستقرة وسلسة.",
    videoToolsTitle: "أدوات الفيديو",
    videoToolsOpenLabel: "فتح أدوات الفيديو",
    videoToolsCloseLabel: "إغلاق أدوات الفيديو",
    videoFileLabel: "اختيار فيديو من الجهاز",
    videoYoutubeLabel: "رابط يوتيوب أو بحث",
    videoYoutubePlaceholder: "الصق رابط يوتيوب أو اكتب اسم الفيلم/الحلقة",
    videoUploadBtn: "رفع الفيديو",
    videoYoutubeBtn: "تشغيل من يوتيوب",
    videoClearBtn: "تخطي الفيديو",
    videoUploadBusy: "جارٍ رفع الفيديو...",
    videoYoutubeBusy: "جارٍ جلب الفيديو...",
    videoUploadNeedFile: "اختر ملف فيديو أولًا.",
    videoYoutubeNeedInput: "أدخل رابط يوتيوب أو نص بحث أولًا.",
    videoYoutubeSuccess: "تم تشغيل فيديو يوتيوب.",
    videoYoutubeNotFound: "لم يتم العثور على نتيجة مناسبة في يوتيوب.",
    videoYoutubeResolveFail: "تعذر جلب نتائج يوتيوب الآن.",
    videoUploadSuccess: "تم رفع الفيديو وتحديث المزامنة.",
    videoClearSuccess: "تم تخطي الفيديو الحالي.",
    videoUploadTooLarge: "حجم الفيديو كبير جدًا (الحد 1GB).",
    videoUploadType: "نوع الفيديو غير مدعوم. استخدم MP4 أو WebM أو OGG.",
    videoUploadNetworkError: "تعذر رفع الفيديو بسبب مشكلة اتصال. تأكد من الإنترنت وحاول مرة أخرى.",
    videoUploadModalTitle: "رفع الفيديو",
    videoUploadModalHint: "يرجى الانتظار حتى يكتمل الرفع، سيتم الإغلاق تلقائيا.",
    videoUploadModalFileLabel: "الملف",
    videoUploadModalProgressLabel: "المرفوع",
    videoUploadModalPercentLabel: "النسبة",
    videoUploadModalProgressValue: "{loaded} / {total} MB",
    videoUploadModalProgressUnknown: "{loaded} MB",
    videoUploadModalPreparing: "جاري تجهيز الرفع...",
    videoUploadModalUploading: "جاري رفع البيانات...",
    videoUploadModalProcessing: "تم رفع البيانات، جارٍ المعالجة...",
    videoUploadModalDone: "اكتمل رفع الفيديو.",
    videoQueueTitle: "طابور الفيديو",
    videoQueueHistoryText: "المشغّل سابقًا: {count}",
    videoQueuePrevBtn: "السابق",
    videoQueueNextBtn: "التالي",
    videoQueueClearBtn: "تفريغ الطابور",
    videoQueueEmpty: "الطابور فارغ.",
    videoQueueQueuedUpload: "تمت إضافة الفيديو إلى الطابور.",
    videoQueueQueuedYoutube: "تمت إضافة فيديو يوتيوب إلى الطابور.",
    videoQueueNextDone: "تم تشغيل العنصر التالي.",
    videoQueueAutoNextCountdown: "الفيديو القادم سيبدأ بعد {seconds} ثوانٍ",
    videoQueuePrevDone: "تم الرجوع إلى الفيديو السابق.",
    videoQueueClearDone: "تم تفريغ الطابور.",
    videoQueueRemoveDone: "تم حذف العنصر من الطابور.",
    videoQueueRemoveItem: "حذف من الطابور",
    videoQueueErrorEmpty: "الطابور فارغ.",
    videoQueueErrorHistoryEmpty: "لا يوجد سجل سابق.",
    videoQueueErrorItemNotFound: "العنصر غير موجود.",
    requestNetworkError: "تعذر الاتصال بالخادم. تأكد من الإنترنت وحاول مرة أخرى.",
    videoHostOnly: "التحكم بالفيديو متاح لقائد الغرفة فقط.",
    videoSyncFailed: "تعذر مزامنة الفيديو.",
    videoControlLocked: "المزامنة بيد قائد الغرفة.",
    videoIncomingMessage: "رسالة جديدة من {user}",
    videoPlayBtn: "تشغيل",
    videoPauseBtn: "إيقاف",
    videoMuteBtn: "كتم الصوت",
    videoUnmuteBtn: "تشغيل الصوت",
    videoVolumeLabel: "صوت الفيديو (محلي)",
    videoFullscreenBtn: "ملء الشاشة",
    videoExitFullscreenBtn: "خروج",
    replyBtn: "رد",
    reactBtn: "تفاعل",
    reactPickOne: "اختيار تفاعل {emoji}",
    replyingTo: "الرد على {user}",
    replyCancel: "إلغاء الرد",
    playersTitle: "المشاهدون",
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
    joinModalTimer: "يمكنك الإغلاق الآن والطلب سيبقى في القائمة.",
    selfKickJokeTitle: "تنبيه",
    selfKickJokeText: "لا يمكنك طرد نفسك.",
    selfKickJokeLobbyBtn: "العودة إلى الصفحة الرئيسية",
    kickSupervisorDeniedTitle: "مرفوض",
    kickSupervisorDeniedText: "لا يمكن طرد المشرف من الغرفة.",
    kickSupervisorDeniedOkBtn: "إغلاق",
    playerLeaderSuffix: "القائد",
    kickBtn: "طرد",
    transferLeaderBtn: "نقل القيادة",
    claimLeaderBtn: "امتلاك القيادة",
    transferLeaderTitle: "نقل القيادة",
    transferLeaderConfirm: "هل تريد نقل القيادة إلى {user}؟",
    claimLeaderTitle: "امتلاك القيادة",
    claimLeaderConfirm: "هل تريد امتلاك القيادة الآن؟",
    systemName: "النظام",
    toastRequestFailed: "فشل الطلب.",
    toastKickedOk: "تمت إزالة {player} من الغرفة.",
    toastLeaderTransferred: "تم نقل القيادة إلى {user}.",
    toastLeaderClaimed: "أصبحت قائد الغرفة الآن.",
    toastJoinApproved: "تم قبول {user}.",
    toastJoinRejected: "تم رفض {user}.",
    toastLoginFirst: "يرجى تسجيل الدخول أولًا.",
    toastInvalidCode: "رمز الغرفة غير صالح.",
    toastKickedOut: "تم طردك من هذه الغرفة ويمكنك إرسال طلب انضمام جديد.",
    toastRoomClosed: "تم إغلاق الغرفة لعدم وجود أعضاء.",
    toastAccountBanned: "تم حظر حسابك من الموقع.",
    sysCreated: "{user} أنشأ الغرفة.",
    sysJoined: "{user} انضم إلى الغرفة.",
    sysKicked: "تم طرد {user} بواسطة القائد.",
    sysLeft: "{user} غادر الغرفة.",
    sysHostChanged: "{user} أصبح القائد الجديد.",
    sysVideoSet: "{user} شغّل الفيديو: {name}.",
    sysVideoRemoved: "{user} حذف الفيديو الحالي.",
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
    profileAvatarTooLarge: "الصورة كبيرة جدًا. اختر صورة أصغر أو أقل دقة.",
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
    pageTitle: "SawaWatch Room",
    langLabel: "Language",
    codeLabel: "Code",
    leaderLabel: "Leader",
    youLabel: "You",
    backToLobby: "Leave Room",
    quickLeave: "Leave",
    chatTitle: "Room Chat",
    chatPlaceholder: "Type your message...",
    sendBtn: "Send",
    chatComposerOpen: "Open typing",
    chatComposerClose: "Hide typing",
    videoTitle: "Room Video",
    videoNoVideo: "No video uploaded yet.",
    videoNowPlaying: "Current video: {name}",
    videoNowPlayingWithUser: "Current video: {name} • by {user}",
    videoByUserInline: "• by {user}",
    videoHint: "All members can add videos, while sync controls stay with the room leader.",
    videoEmptyLine1: "This site was created to make watching movies and videos easier.",
    videoEmptyLine2: "Built for smooth and reliable group watching sessions.",
    videoToolsTitle: "Video Tools",
    videoToolsOpenLabel: "Open video tools",
    videoToolsCloseLabel: "Close video tools",
    videoFileLabel: "Choose video from device",
    videoYoutubeLabel: "YouTube link or search",
    videoYoutubePlaceholder: "Paste a YouTube URL or type a movie/episode title",
    videoUploadBtn: "Upload Video",
    videoYoutubeBtn: "Play from YouTube",
    videoClearBtn: "Skip Video",
    videoUploadBusy: "Uploading...",
    videoYoutubeBusy: "Resolving video...",
    videoUploadNeedFile: "Choose a video file first.",
    videoYoutubeNeedInput: "Enter a YouTube URL or search text first.",
    videoYoutubeSuccess: "YouTube video started.",
    videoYoutubeNotFound: "No suitable YouTube result was found.",
    videoYoutubeResolveFail: "Could not resolve YouTube right now.",
    videoUploadSuccess: "Video uploaded and sync updated.",
    videoClearSuccess: "Current video skipped.",
    videoUploadTooLarge: "Video is too large (max 1GB).",
    videoUploadType: "Unsupported video type. Use MP4, WebM, or OGG.",
    videoUploadNetworkError: "Video upload failed due to a network issue. Check your connection and try again.",
    videoUploadModalTitle: "Uploading Video",
    videoUploadModalHint: "Please wait until upload finishes. This window closes automatically.",
    videoUploadModalFileLabel: "File",
    videoUploadModalProgressLabel: "Uploaded",
    videoUploadModalPercentLabel: "Percent",
    videoUploadModalProgressValue: "{loaded} / {total} MB",
    videoUploadModalProgressUnknown: "{loaded} MB",
    videoUploadModalPreparing: "Preparing upload...",
    videoUploadModalUploading: "Uploading data...",
    videoUploadModalProcessing: "Upload complete, processing...",
    videoUploadModalDone: "Video upload completed.",
    videoQueueTitle: "Video Queue",
    videoQueueHistoryText: "Played before: {count}",
    videoQueuePrevBtn: "Previous",
    videoQueueNextBtn: "Next",
    videoQueueClearBtn: "Clear Queue",
    videoQueueEmpty: "Queue is empty.",
    videoQueueQueuedUpload: "Video added to queue.",
    videoQueueQueuedYoutube: "YouTube video added to queue.",
    videoQueueNextDone: "Moved to next queued video.",
    videoQueueAutoNextCountdown: "Next video starts in {seconds}s",
    videoQueuePrevDone: "Returned to previous video.",
    videoQueueClearDone: "Queue cleared.",
    videoQueueRemoveDone: "Queue item removed.",
    videoQueueRemoveItem: "Remove from queue",
    videoQueueErrorEmpty: "Queue is empty.",
    videoQueueErrorHistoryEmpty: "History is empty.",
    videoQueueErrorItemNotFound: "Queue item was not found.",
    requestNetworkError: "Could not reach the server. Check your connection and try again.",
    videoHostOnly: "Video controls are leader-only.",
    videoSyncFailed: "Failed to sync video.",
    videoControlLocked: "Video sync is controlled by room leader.",
    videoIncomingMessage: "New message from {user}",
    videoPlayBtn: "Play",
    videoPauseBtn: "Pause",
    videoMuteBtn: "Mute",
    videoUnmuteBtn: "Unmute",
    videoVolumeLabel: "Video volume (local)",
    videoFullscreenBtn: "Fullscreen",
    videoExitFullscreenBtn: "Exit",
    replyBtn: "Reply",
    reactBtn: "React",
    reactPickOne: "Choose reaction {emoji}",
    replyingTo: "Replying to {user}",
    replyCancel: "Cancel reply",
    playersTitle: "Viewers",
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
    joinModalTimer: "You can close now and keep this request pending.",
    selfKickJokeTitle: "Warning",
    selfKickJokeText: "You cannot kick yourself.",
    selfKickJokeLobbyBtn: "Back to Home",
    kickSupervisorDeniedTitle: "Denied",
    kickSupervisorDeniedText: "You cannot kick a supervisor from this room.",
    kickSupervisorDeniedOkBtn: "Close",
    playerLeaderSuffix: "Leader",
    kickBtn: "Kick",
    transferLeaderBtn: "Transfer Leader",
    claimLeaderBtn: "Claim Leader",
    transferLeaderTitle: "Transfer Leadership",
    transferLeaderConfirm: "Transfer leadership to {user}?",
    claimLeaderTitle: "Claim Leadership",
    claimLeaderConfirm: "Do you want to claim leadership now?",
    systemName: "System",
    toastRequestFailed: "Request failed.",
    toastKickedOk: "{player} was removed from the watch room.",
    toastLeaderTransferred: "Leadership transferred to {user}.",
    toastLeaderClaimed: "You are now the room leader.",
    toastJoinApproved: "{user} was approved.",
    toastJoinRejected: "{user} was rejected.",
    toastLoginFirst: "Please login first.",
    toastInvalidCode: "Invalid room code.",
    toastKickedOut: "You were kicked from this room and can request to join again.",
    toastRoomClosed: "The room was closed because it became empty.",
    toastAccountBanned: "Your account was banned from the site.",
    sysCreated: "{user} created the room.",
    sysJoined: "{user} joined the room.",
    sysKicked: "{user} was kicked by the leader.",
    sysLeft: "{user} left the room.",
    sysHostChanged: "{user} is now the leader.",
    sysVideoSet: "{user} started video: {name}.",
    sysVideoRemoved: "{user} removed the current video.",
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
    profileAvatarTooLarge: "Image is too large. Choose a smaller one.",
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
    messageReactionState.clear();
    closeReactionPickers();
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
  soundToggleBtn.textContent = t("soundOn");
  soundToggleBtn.classList.remove("off");
  soundToggleBtn.disabled = true;
  soundToggleBtn.setAttribute("aria-disabled", "true");
  soundToggleBtn.title = t("soundOn");
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

function focusChatInputForContinuousTyping(submitStartedAt = 0, { force = false } = {}) {
  if (!chatInput || !chatForm) {
    return;
  }
  if (!force && Number(submitStartedAt) > 0 && chatLastOutsidePointerAt > Number(submitStartedAt)) {
    return;
  }
  const active = document.activeElement;
  if (!force && (
    active &&
    active !== document.body &&
    active !== document.documentElement &&
    active !== chatInput &&
    !(chatForm.contains(active))
  )) {
    return;
  }
  const runFocus = () => {
    try {
      chatInput.focus({ preventScroll: true });
      const len = Number(chatInput.value?.length || 0);
      if (typeof chatInput.setSelectionRange === "function") {
        chatInput.setSelectionRange(len, len);
      }
    } catch (_error) {
      // Ignore focus errors.
    }
  };
  if (typeof requestAnimationFrame === "function") {
    requestAnimationFrame(runFocus);
  } else {
    setTimeout(runFocus, 0);
  }
  if (!isRoomMobileLayout()) {
    return;
  }
  // Mobile keyboards may close on submit; retry focus briefly to keep typing flow.
  [60, 150, 280].forEach((delayMs) => {
    setTimeout(runFocus, delayMs);
  });
}

function setMobileChatKeyboardOffset(offsetPx, { keepLatestVisible = false } = {}) {
  const numericOffset = Number(offsetPx);
  const safeOffset = Number.isFinite(numericOffset)
    ? Math.max(0, Math.min(520, Math.round(numericOffset)))
    : 0;
  const changed = safeOffset !== mobileChatKeyboardOffsetPx;
  mobileChatKeyboardOffsetPx = safeOffset;
  document.documentElement.style.setProperty("--room-chat-keyboard-offset", `${safeOffset}px`);
  document.body.classList.toggle("room-mobile-keyboard-open", safeOffset > 0);
  if (!chatMessages) {
    return;
  }
  if (!changed && !keepLatestVisible) {
    return;
  }
  const scrollToLatest = () => {
    chatMessages.scrollTop = chatMessages.scrollHeight;
  };
  if (typeof requestAnimationFrame === "function") {
    requestAnimationFrame(scrollToLatest);
  } else {
    setTimeout(scrollToLatest, 0);
  }
}

function getMobileChatKeyboardOffset() {
  if (!isRoomMobileLayout()) {
    return 0;
  }
  if (!window.visualViewport) {
    return 0;
  }
  const viewport = window.visualViewport;
  const viewportBottom = Number(viewport.height || 0) + Number(viewport.offsetTop || 0);
  const rawOffset = Math.max(0, Math.round(Number(window.innerHeight || 0) - viewportBottom));
  if (rawOffset < MOBILE_CHAT_KEYBOARD_OPEN_THRESHOLD_PX) {
    return 0;
  }
  return rawOffset;
}

function syncMobileChatKeyboardOffset({ keepLatestVisible = false } = {}) {
  if (!chatForm || !chatMessages) {
    return;
  }
  if (!isRoomMobileLayout()) {
    setMobileChatKeyboardOffset(0, { keepLatestVisible: false });
    return;
  }
  const inputFocused = document.activeElement === chatInput;
  const keyboardOffset = inputFocused ? getMobileChatKeyboardOffset() : 0;
  const shouldStickLatest = keepLatestVisible || keyboardOffset > 0 || inputFocused;
  setMobileChatKeyboardOffset(keyboardOffset, { keepLatestVisible: shouldStickLatest });
}

function bindMobileChatViewportEvents() {
  if (mobileChatViewportEventsBound) {
    return;
  }
  mobileChatViewportEventsBound = true;
  const handleViewportShift = () => {
    syncMobileChatKeyboardOffset({ keepLatestVisible: true });
  };
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", handleViewportShift);
    window.visualViewport.addEventListener("scroll", handleViewportShift);
  }
  window.addEventListener("resize", handleViewportShift, { passive: true });
  window.addEventListener("orientationchange", handleViewportShift);
}

function setMobileChatComposerOpen(open, { focusInput = false, blurInput = false } = {}) {
  if (!chatForm || !chatInput) {
    return;
  }
  const mobile = isRoomMobileLayout();
  void open;
  if (!mobile) {
    mobileChatComposerOpen = true;
    chatForm.classList.remove("mobile-chat-collapsed");
    document.body.classList.remove("mobile-chat-composer-open");
    if (chatComposerOpenBtn) {
      chatComposerOpenBtn.classList.add("hidden");
    }
    if (chatComposerCloseBtn) {
      chatComposerCloseBtn.classList.add("hidden");
    }
    setMobileChatKeyboardOffset(0);
    return;
  }

  mobileChatComposerOpen = true;
  chatForm.classList.remove("mobile-chat-collapsed");
  document.body.classList.add("mobile-chat-composer-open");
  if (chatComposerOpenBtn) {
    chatComposerOpenBtn.classList.add("hidden");
  }
  if (chatComposerCloseBtn) {
    chatComposerCloseBtn.classList.add("hidden");
  }
  if (blurInput && !focusInput) {
    chatInput.blur();
  }
  if (focusInput) {
    focusChatInputForContinuousTyping(0, { force: true });
  }
  syncMobileChatKeyboardOffset({ keepLatestVisible: true });
}

function closeMobileChatComposerFromUserAction() {
  if (!isRoomMobileLayout()) {
    return;
  }
  if (chatInput) {
    chatInput.blur();
  }
  [0, 90, 180].forEach((delayMs) => {
    setTimeout(() => {
      syncMobileChatKeyboardOffset({ keepLatestVisible: true });
    }, delayMs);
  });
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

function normalizeMessageReactionsState(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const normalized = {};
  CHAT_REACTION_CHOICES.forEach((emoji) => {
    const usersRaw = value[emoji];
    if (!Array.isArray(usersRaw) || usersRaw.length === 0) {
      return;
    }
    const users = [];
    const seen = new Set();
    usersRaw.forEach((entry) => {
      const username = String(entry || "").trim();
      if (!username || seen.has(username)) {
        return;
      }
      seen.add(username);
      users.push(username);
    });
    if (users.length > 0) {
      normalized[emoji] = users;
    }
  });
  return normalized;
}

function getMessageReactionsState(messageId) {
  const id = Number(messageId || 0);
  if (!id) {
    return {};
  }
  return messageReactionState.get(id) || {};
}

function setMessageReactionsState(messageId, value) {
  const id = Number(messageId || 0);
  if (!id) {
    return {};
  }
  const normalized = normalizeMessageReactionsState(value);
  if (Object.keys(normalized).length === 0) {
    messageReactionState.delete(id);
    return {};
  }
  messageReactionState.set(id, normalized);
  return normalized;
}

function closeReactionPickers(exceptMessageId = 0) {
  const keepId = Number(exceptMessageId || 0);
  const lineElements = chatMessages ? chatMessages.querySelectorAll(".chat-line-message[data-message-id]") : [];
  lineElements.forEach((lineWrap) => {
    const lineMessageId = Number(lineWrap.dataset.messageId || 0);
    if (keepId && lineMessageId === keepId) {
      return;
    }
    const picker = lineWrap.querySelector(".chat-line-reaction-picker");
    const reactBtn = lineWrap.querySelector(".chat-line-react-btn");
    if (picker) {
      picker.classList.add("hidden");
      picker.classList.remove("is-open");
    }
    if (reactBtn) {
      reactBtn.setAttribute("aria-expanded", "false");
    }
  });
  if (!keepId) {
    activeReactionPickerMessageId = 0;
  }
}

function toggleReactionPicker(lineWrap, messageId) {
  if (!lineWrap) {
    return;
  }
  const lineMessageId = Number(messageId || lineWrap.dataset.messageId || 0);
  if (!lineMessageId) {
    return;
  }
  const picker = lineWrap.querySelector(".chat-line-reaction-picker");
  const reactBtn = lineWrap.querySelector(".chat-line-react-btn");
  if (!picker || !reactBtn) {
    return;
  }
  const shouldOpen = picker.classList.contains("hidden");
  closeReactionPickers(shouldOpen ? lineMessageId : 0);
  if (!shouldOpen) {
    return;
  }
  picker.classList.remove("hidden");
  picker.classList.add("is-open");
  reactBtn.setAttribute("aria-expanded", "true");
  activeReactionPickerMessageId = lineMessageId;
}

function reactionChipTitle(emoji, users) {
  const list = Array.isArray(users) ? users : [];
  if (list.length === 0) {
    return emoji;
  }
  const preview = list.slice(0, 4).map((username) => replyTargetDisplayName(username)).join(", ");
  const suffix = list.length > 4 ? ` +${list.length - 4}` : "";
  return `${emoji} ${preview}${suffix}`;
}

function renderMessageReactionChips(lineWrap, messageId) {
  if (!lineWrap) {
    return;
  }
  const row = lineWrap.querySelector(".chat-line-reactions");
  if (!row) {
    return;
  }
  const reactions = getMessageReactionsState(messageId);
  row.innerHTML = "";
  CHAT_REACTION_CHOICES.forEach((emoji) => {
    const users = Array.isArray(reactions[emoji]) ? reactions[emoji] : [];
    if (users.length === 0) {
      return;
    }
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chat-line-reaction-chip";
    chip.dataset.emoji = emoji;
    chip.title = reactionChipTitle(emoji, users);
    chip.setAttribute("aria-label", fmt(t("reactPickOne"), { emoji }));
    if (users.includes(me)) {
      chip.classList.add("is-active");
    }
    const emojiText = document.createElement("span");
    emojiText.className = "chat-line-reaction-emoji";
    emojiText.textContent = emoji;
    const countText = document.createElement("span");
    countText.className = "chat-line-reaction-count";
    countText.textContent = String(users.length);
    chip.appendChild(emojiText);
    chip.appendChild(countText);
    chip.addEventListener("click", () => {
      sfx("click");
      toggleMessageReaction(messageId, emoji);
    });
    row.appendChild(chip);
  });
  row.classList.toggle("hidden", row.childElementCount === 0);
}

function updateLineReactionButton(lineWrap, messageId) {
  if (!lineWrap) {
    return;
  }
  const reactBtn = lineWrap.querySelector(".chat-line-react-btn");
  if (!reactBtn) {
    return;
  }
  const reactions = getMessageReactionsState(messageId);
  const hasMyReaction = Object.values(reactions).some(
    (users) => Array.isArray(users) && users.includes(me)
  );
  reactBtn.classList.toggle("has-my-reaction", hasMyReaction);
  reactBtn.setAttribute("aria-pressed", hasMyReaction ? "true" : "false");
}

function updateMessageReactionUi(messageId) {
  const id = Number(messageId || 0);
  if (!id || !chatMessages) {
    return;
  }
  const lines = chatMessages.querySelectorAll(`.chat-line-message[data-message-id="${id}"]`);
  lines.forEach((lineWrap) => {
    renderMessageReactionChips(lineWrap, id);
    updateLineReactionButton(lineWrap, id);
  });
}

function applyMessageReactionUpdate(payload) {
  const messageId = Number(payload?.messageId || 0);
  if (!messageId) {
    return;
  }
  setMessageReactionsState(messageId, payload?.reactions);
  updateMessageReactionUi(messageId);
}

async function toggleMessageReaction(messageId, emoji) {
  const targetMessageId = Number(messageId || 0);
  const targetEmoji = CHAT_REACTION_CHOICES.includes(String(emoji || "").trim())
    ? String(emoji).trim()
    : "";
  if (!targetMessageId || !targetEmoji) {
    return;
  }
  const inFlightKey = `${targetMessageId}:${targetEmoji}`;
  if (reactionToggleInFlight.has(inFlightKey)) {
    return;
  }
  reactionToggleInFlight.add(inFlightKey);
  try {
    const result = await api(`/api/rooms/${encodedCode}/messages`, {
      method: "POST",
      body: {
        action: "react",
        messageId: targetMessageId,
        emoji: targetEmoji
      }
    });
    setMessageReactionsState(targetMessageId, result?.reactions);
    updateMessageReactionUi(targetMessageId);
  } catch (error) {
    showToast(error.message);
  } finally {
    reactionToggleInFlight.delete(inFlightKey);
  }
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

function countryCodeForUser(username) {
  const profileCountry = normalizeCountryCode(profileCache.get(username)?.countryCode);
  if (profileCountry) {
    return profileCountry;
  }
  if (username === me) {
    return CLIENT_COUNTRY_CODE;
  }
  return "";
}

function displayNameFor(username) {
  const profile = profileCache.get(username);
  return profile?.displayName || username;
}

function videoUploaderName(username) {
  const clean = String(username || "").trim();
  if (!clean) {
    return "";
  }
  return displayNameFor(clean);
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
    const profileCountryCode = profile.countryCode || (isSelf ? CLIENT_COUNTRY_CODE : "");
    const countryFlagBadge = createCountryFlagBadge(profileCountryCode);
    if (countryFlagBadge) {
      profileUsername.appendChild(countryFlagBadge);
    }
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

function placeVideoToolsInBody(mobile) {
  if (mobile) {
    if (videoLeaderTools && videoLeaderTools.parentNode !== document.body) {
      document.body.appendChild(videoLeaderTools);
    }
    if (videoToolsOverlay && videoToolsOverlay.parentNode !== document.body) {
      document.body.appendChild(videoToolsOverlay);
    }
    return;
  }
  if (videoLeaderTools && videoToolsOriginalParent && videoLeaderTools.parentNode !== videoToolsOriginalParent) {
    videoToolsOriginalParent.insertBefore(videoLeaderTools, videoToolsOriginalNextSibling);
  }
  if (videoToolsOverlay && videoToolsOverlayOriginalParent && videoToolsOverlay.parentNode !== videoToolsOverlayOriginalParent) {
    videoToolsOverlayOriginalParent.insertBefore(videoToolsOverlay, videoToolsOverlayOriginalNextSibling);
  }
}

function clearVideoToolsModalHideTimer() {
  if (!videoToolsModalHideTimer) {
    return;
  }
  clearTimeout(videoToolsModalHideTimer);
  videoToolsModalHideTimer = null;
}

function setVideoToolsModalStyle(open) {
  if (!videoLeaderTools) {
    return;
  }
  clearVideoToolsModalHideTimer();
  if (!open) {
    videoLeaderTools.style.setProperty("opacity", "0", "important");
    videoLeaderTools.style.setProperty("transform", "translate(-50%, -47%) scale(0.96)", "important");
    videoLeaderTools.style.setProperty("pointer-events", "none", "important");
    videoToolsModalHideTimer = setTimeout(() => {
      if (!videoToolsOpen) {
        videoLeaderTools.style.setProperty("display", "none", "important");
      }
    }, 180);
    return;
  }
  videoLeaderTools.style.setProperty("display", "grid", "important");
  videoLeaderTools.style.setProperty("position", "fixed", "important");
  videoLeaderTools.style.setProperty("left", "50%", "important");
  videoLeaderTools.style.setProperty("top", "50%", "important");
  videoLeaderTools.style.setProperty("right", "auto", "important");
  videoLeaderTools.style.setProperty("bottom", "auto", "important");
  videoLeaderTools.style.setProperty("transform", "translate(-50%, -50%)", "important");
  videoLeaderTools.style.setProperty("width", "min(92vw, 460px)", "important");
  videoLeaderTools.style.setProperty("max-height", "min(72dvh, 560px)", "important");
  videoLeaderTools.style.setProperty("overflow", "auto", "important");
  videoLeaderTools.style.setProperty("z-index", "160", "important");
  videoLeaderTools.style.setProperty("padding", "0.95rem", "important");
  videoLeaderTools.style.setProperty("border-radius", "16px", "important");
  videoLeaderTools.style.setProperty("border", "1px solid rgba(132, 183, 255, 0.34)", "important");
  videoLeaderTools.style.setProperty("transition", "opacity 0.18s ease, transform 0.22s ease", "important");
  videoLeaderTools.style.setProperty(
    "background",
    "linear-gradient(165deg, rgba(8, 18, 38, 0.97), rgba(10, 24, 49, 0.98))",
    "important"
  );
  videoLeaderTools.style.setProperty("box-shadow", "0 26px 52px rgba(1, 8, 21, 0.68)", "important");
  videoLeaderTools.style.setProperty("opacity", "0", "important");
  videoLeaderTools.style.setProperty("transform", "translate(-50%, -47%) scale(0.96)", "important");
  videoLeaderTools.style.setProperty("pointer-events", "none", "important");
  requestAnimationFrame(() => {
    if (!videoToolsOpen) {
      return;
    }
    videoLeaderTools.style.setProperty("opacity", "1", "important");
    videoLeaderTools.style.setProperty("transform", "translate(-50%, -50%) scale(1)", "important");
    videoLeaderTools.style.setProperty("pointer-events", "auto", "important");
  });
}

function triggerAutoPlayForNewRoomVideo() {
  if (!isRoomLeader()) {
    return;
  }
  let started = false;
  const run = () => {
    if (started || !isRoomLeader() || !roomVideoState) {
      return;
    }
    if (isYouTubeRoomVideo(roomVideoState)) {
      if (!youTubePlayerReady || !youTubePlayer) {
        youTubeLeaderPlayPending = true;
        return;
      }
      youTubeLeaderPlayPending = false;
      started = true;
      if (typeof youTubePlayer.seekTo === "function") {
        try {
          youTubePlayer.seekTo(0, true);
        } catch (_error) {
          // Ignore seek failures and continue sync request.
        }
      }
      if (typeof youTubePlayer.playVideo === "function") {
        try {
          youTubePlayer.playVideo();
        } catch (_error) {
          // Ignore and continue sync request.
        }
      }
      sendRoomVideoSync("play", {
        currentTimeOverride: 0,
        allowRewind: true,
        quietError: true
      });
      return;
    }
    if (roomVideoPlayer) {
      started = true;
      try {
        roomVideoPlayer.currentTime = 0;
      } catch (_error) {
        // Ignore currentTime setter errors on not-ready media.
      }
      roomVideoPlayer.play().catch(() => {});
      sendRoomVideoSync("play", {
        currentTimeOverride: 0,
        allowRewind: true,
        quietError: true
      });
    }
  };
  run();
  setTimeout(run, 350);
  setTimeout(run, 900);
}

function setVideoToolsDrawerOpen(open) {
  const mobile = isRoomMobileLayout();
  placeVideoToolsInBody(mobile);
  const canControl = canUseVideoTools();
  const canOpen = mobile && canControl;
  const nextOpen = Boolean(open && canOpen);
  videoToolsOpen = nextOpen;
  if (videoLeaderTools) {
    if (mobile) {
      videoLeaderTools.classList.toggle("is-open", nextOpen);
      if (canOpen) {
        videoLeaderTools.classList.remove("hidden");
      } else {
        videoLeaderTools.classList.add("hidden");
      }
      setVideoToolsModalStyle(nextOpen);
      videoLeaderTools.setAttribute("aria-hidden", canOpen ? (nextOpen ? "false" : "true") : "true");
    } else {
      videoLeaderTools.classList.remove("is-open");
      videoLeaderTools.classList.toggle("hidden", !canControl);
      videoLeaderTools.removeAttribute("style");
      videoLeaderTools.setAttribute("aria-hidden", canControl ? "false" : "true");
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
  if (videoToolsClose) {
    videoToolsClose.classList.toggle("hidden", !mobile);
  }
  placeVideoToolsInBody(mobile);
  playersDrawerToggle.classList.toggle("hidden", !mobile);
  if (!mobile) {
    playersDrawerOpen = false;
    videoToolsOpen = false;
    const canControlVideo = canUseVideoTools();
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
      videoLeaderTools.classList.toggle("hidden", !canControlVideo);
      videoLeaderTools.removeAttribute("style");
      videoLeaderTools.setAttribute("aria-hidden", canControlVideo ? "false" : "true");
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
    updateVideoClearButtonState();
    setMobileChatComposerOpen(true);
    syncRoomBodyLock();
    return;
  }
  setPlayersDrawerOpen(playersDrawerOpen);
  setVideoToolsDrawerOpen(videoToolsOpen);
  setMobileChatComposerOpen(mobileChatComposerOpen);
  updateVideoClearButtonState();
}

function isRoomLeader() {
  return Boolean(me && host && me === host);
}

function canUseVideoTools() {
  return Boolean(me);
}

function parseYouTubeVideoId(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (/^[A-Za-z0-9_-]{11}$/.test(raw)) {
    return raw;
  }
  try {
    const parsed = new URL(raw);
    const host = String(parsed.hostname || "").toLowerCase();
    const parts = String(parsed.pathname || "")
      .split("/")
      .filter(Boolean);
    if (host === "youtu.be") {
      const shortId = String(parts[0] || "").trim();
      return /^[A-Za-z0-9_-]{11}$/.test(shortId) ? shortId : "";
    }
    if (host === "youtube.com" || host.endsWith(".youtube.com") || host === "music.youtube.com") {
      const watchId = String(parsed.searchParams.get("v") || "").trim();
      if (/^[A-Za-z0-9_-]{11}$/.test(watchId)) {
        return watchId;
      }
      const first = String(parts[0] || "").trim();
      const second = String(parts[1] || "").trim();
      if ((first === "embed" || first === "shorts" || first === "live") && /^[A-Za-z0-9_-]{11}$/.test(second)) {
        return second;
      }
    }
  } catch (_error) {
    // Non-URL value, continue to regex fallbacks.
  }
  const watchMatch = raw.match(/[?&]v=([A-Za-z0-9_-]{11})/i);
  if (watchMatch) {
    return watchMatch[1];
  }
  const embedMatch = raw.match(/\/(?:embed|shorts|live)\/([A-Za-z0-9_-]{11})/i);
  if (embedMatch) {
    return embedMatch[1];
  }
  const shortMatch = raw.match(/youtu\.be\/([A-Za-z0-9_-]{11})/i);
  if (shortMatch) {
    return shortMatch[1];
  }
  return "";
}

function stopYouTubeTick() {
  if (!youTubeTickTimer) {
    return;
  }
  clearInterval(youTubeTickTimer);
  youTubeTickTimer = null;
}

function startYouTubeTick() {
  stopYouTubeTick();
  youTubeTickTimer = setInterval(() => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      updateRoomVideoControls();
    }
  }, 300);
}

function suppressYouTubeLeaderStateBroadcastTemporarily(durationMs = 900) {
  if (!isRoomLeader()) {
    return;
  }
  suppressYouTubeStateBroadcast = true;
  setTimeout(() => {
    if (!youTubeInitialSyncPending) {
      suppressYouTubeStateBroadcast = false;
    }
  }, Math.max(120, Number(durationMs) || 0));
}

function loadYouTubeApi() {
  if (window.YT && typeof window.YT.Player === "function") {
    return Promise.resolve();
  }
  if (youTubeApiReadyPromise) {
    return youTubeApiReadyPromise;
  }
  youTubeApiReadyPromise = new Promise((resolve, reject) => {
    const previousReady = window.onYouTubeIframeAPIReady;
    window.onYouTubeIframeAPIReady = () => {
      if (typeof previousReady === "function") {
        try {
          previousReady();
        } catch (_) {
          // Ignore callback chaining errors from other scripts.
        }
      }
      resolve();
    };
    const script = document.createElement("script");
    script.src = "https://www.youtube.com/iframe_api";
    script.async = true;
    script.onerror = () => reject(new Error("YOUTUBE_API_LOAD_FAILED"));
    document.head.appendChild(script);
  });
  return youTubeApiReadyPromise;
}

function destroyYouTubePlayer() {
  stopYouTubeTick();
  if (youTubePlayer) {
    try {
      if (typeof youTubePlayer.destroy === "function") {
        youTubePlayer.destroy();
      } else if (typeof youTubePlayer.stopVideo === "function") {
        youTubePlayer.stopVideo();
      } else if (typeof youTubePlayer.pauseVideo === "function") {
        youTubePlayer.pauseVideo();
      }
    } catch (_) {
      // Ignore teardown errors.
    }
  }
  if (roomVideoEmbed) {
    roomVideoEmbed.innerHTML = "";
  }
  youTubePlayer = null;
  youTubePlayerReady = false;
  youTubeState = -1;
  activeYouTubeVideoId = "";
  youTubeAudioUnlockNeeded = false;
  youTubeInitialSyncPending = false;
  youTubeLeaderPlayPending = false;
  setYouTubeMaskVisible(false);
}

function hardenYouTubeIframeUi() {
  if (!youTubePlayer || typeof youTubePlayer.getIframe !== "function") {
    return;
  }
  const iframe = youTubePlayer.getIframe();
  if (!iframe) {
    return;
  }
  iframe.setAttribute("tabindex", "-1");
  iframe.setAttribute("aria-hidden", "true");
  iframe.style.pointerEvents = "none";
}

function shouldUnlockYouTubeAudio() {
  return Boolean(
    youTubePlayer
      && youTubePlayerReady
      && roomVideoUserInteracted
      && isYouTubeRoomVideo(roomVideoState)
  );
}

function ensureMutedAutoplayForYouTube(player = youTubePlayer) {
  if (!player || roomVideoUserInteracted) {
    return;
  }
  try {
    if (typeof player.mute === "function") {
      player.mute();
    }
  } catch (_) {
    // Best effort; ignore mute failures.
  }
}

function markRoomVideoUserInteracted() {
  if (roomVideoUserInteracted) {
    return;
  }
  roomVideoUserInteracted = true;
  if (roomVideoPlayer) {
    const effectiveVolume = getEffectiveRoomVideoVolume();
    roomVideoPlayer.volume = effectiveVolume;
    roomVideoPlayer.muted = effectiveVolume <= 0.001;
    if (effectiveVolume > 0.001 && roomVideoSyncState?.playing && !isYouTubeRoomVideo(roomVideoState)) {
      roomVideoPlayer.play().catch(() => {});
    }
  }
  applyLocalRoomVideoVolumeToPlayers();
}

function tryUnlockYouTubeAudioFromGesture() {
  if (!shouldUnlockYouTubeAudio()) {
    return false;
  }
  const effectiveVolume = getEffectiveRoomVideoVolume();
  const targetPercent = Math.round(clampRoomVideoVolume(effectiveVolume) * 100);
  try {
    if (typeof youTubePlayer.setVolume === "function") {
      youTubePlayer.setVolume(targetPercent);
    }
    if (targetPercent <= 0) {
      if (typeof youTubePlayer.mute === "function") {
        youTubePlayer.mute();
      }
      youTubeAudioUnlockNeeded = false;
      return true;
    }
    if (typeof youTubePlayer.unMute === "function") {
      youTubePlayer.unMute();
    }
    if (roomVideoSyncState?.playing && typeof youTubePlayer.playVideo === "function") {
      youTubePlayer.playVideo();
    }
    if (typeof youTubePlayer.isMuted === "function") {
      youTubeAudioUnlockNeeded = Boolean(youTubePlayer.isMuted());
    } else {
      youTubeAudioUnlockNeeded = false;
    }
  } catch (_) {
    return false;
  }
  return !youTubeAudioUnlockNeeded;
}

function attemptMobileRoomVideoResume({ fromGesture = false } = {}) {
  if (!roomVideoResumePending) {
    return;
  }
  if (!roomVideoState || !roomVideoSyncState || !roomVideoSyncState.playing) {
    roomVideoResumePending = false;
    return;
  }

  if (isYouTubeRoomVideo(roomVideoState)) {
    if (!youTubePlayer || !youTubePlayerReady) {
      return;
    }
    try {
      if (typeof youTubePlayer.playVideo === "function") {
        ensureMutedAutoplayForYouTube(youTubePlayer);
        youTubePlayer.playVideo();
      }
      if (fromGesture) {
        youTubeAudioUnlockNeeded = true;
        tryUnlockYouTubeAudioFromGesture();
      }
    } catch (_) {
      // Ignore resume failures and retry on the next interaction.
    }
    applyRoomVideoSyncToPlayer({ forceSeek: true });
    if ((isYouTubePlaying() && !youTubeAudioUnlockNeeded) || (fromGesture && !youTubeAudioUnlockNeeded)) {
      roomVideoResumePending = false;
    }
    return;
  }

  if (!roomVideoPlayer) {
    return;
  }
  try {
    if (fromGesture && roomVideoPlayer.muted) {
      roomVideoPlayer.muted = false;
    }
    roomVideoPlayer.play().catch(() => {});
  } catch (_) {
    // Ignore resume failures and retry on the next interaction.
  }
  applyRoomVideoSyncToPlayer({ forceSeek: true });
  if (!roomVideoPlayer.paused) {
    roomVideoResumePending = false;
  }
}

function getYouTubeDuration() {
  if (!youTubePlayer || !youTubePlayerReady || typeof youTubePlayer.getDuration !== "function") {
    return 0;
  }
  const value = Number(youTubePlayer.getDuration() || 0);
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function getYouTubeCurrentTime() {
  if (!youTubePlayer || !youTubePlayerReady || typeof youTubePlayer.getCurrentTime !== "function") {
    return 0;
  }
  const value = Number(youTubePlayer.getCurrentTime() || 0);
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function isYouTubePlaying() {
  return youTubeState === 1;
}

function setYouTubeMaskVisible(visible) {
  if (!videoYouTubeMask) {
    return;
  }
  const show = Boolean(isYouTubeRoomVideo(roomVideoState) && visible);
  videoYouTubeMask.classList.toggle("hidden", !show);
}

async function ensureYouTubePlayer(videoId, { autoplay = true } = {}) {
  if (!videoId || !roomVideoEmbed) {
    return;
  }
  await loadYouTubeApi();
  if (!window.YT || typeof window.YT.Player !== "function") {
    throw new Error("YOUTUBE_API_LOAD_FAILED");
  }
  if (!youTubePlayer) {
    youTubePlayer = new window.YT.Player("roomVideoEmbed", {
      host: "https://www.youtube-nocookie.com",
      videoId,
      playerVars: {
        autoplay: autoplay ? 1 : 0,
        controls: 0,
        disablekb: 1,
        fs: 0,
        rel: 0,
        modestbranding: 1,
        playsinline: 1,
        iv_load_policy: 3
      },
      events: {
        onReady: (event) => {
          youTubePlayerReady = true;
          youTubeAudioUnlockNeeded = true;
          hardenYouTubeIframeUi();
          youTubeState = Number(event?.target?.getPlayerState?.() ?? -1);
          setYouTubeMaskVisible(youTubeState !== 1);
          applyLocalRoomVideoVolumeToPlayers();
          updateRoomVideoControls();
          startYouTubeTick();
          if (autoplay && event?.target?.playVideo) {
            ensureMutedAutoplayForYouTube(event.target);
            event.target.playVideo();
          }
          if (youTubeInitialSyncPending && roomVideoSyncState) {
            applyRoomVideoSyncToPlayer({ forceSeek: true });
          }
          if (isRoomLeader() && youTubeLeaderPlayPending && event?.target?.playVideo) {
            const current = getSafeYouTubeCurrentTime(getRoomVideoDuration(), { keepMonotonicWhilePlaying: true });
            ensureMutedAutoplayForYouTube(event.target);
            event.target.playVideo();
            sendRoomVideoSync("play", { currentTimeOverride: current, quietError: true });
            youTubeLeaderPlayPending = false;
          }
          tryUnlockYouTubeAudioFromGesture();
        },
        onStateChange: (event) => {
          hardenYouTubeIframeUi();
          youTubeState = Number(event?.data ?? -1);
          setYouTubeMaskVisible(youTubeState !== 1);
          updateRoomVideoControls();
          if (youTubeState === 1) {
            youTubeAudioUnlockNeeded = true;
            tryUnlockYouTubeAudioFromGesture();
            scheduleRoomVideoControlsAutoHide();
          } else {
            setRoomVideoControlsVisible(true);
          }
          if (youTubeState === 0 && roomVideoState && isYouTubeRoomVideo(roomVideoState)) {
            const duration = getYouTubeDuration();
            const current = getSafeYouTubeCurrentTime(duration);
            if (duration > 2 && current < duration - 1.2) {
              applyRoomVideoSyncToPlayer({ forceSeek: true });
              return;
            }
            const endTime = duration > 0 && current >= duration - 0.35 ? duration : current;
            roomVideoLastKnownTime = endTime;
            const endedVideoId = String(activeRoomVideoId || roomVideoState?.id || "").trim();
            if (roomVideoQueueState.length > 0) {
              startRoomVideoAutoNextCountdown(endedVideoId);
              if (isRoomLeader()) {
                handleLeaderVideoEndedAutoAdvance(endTime);
              }
              return;
            }
            cancelRoomVideoAutoNextCountdown();
            if (isRoomLeader()) {
              handleLeaderVideoEndedAutoAdvance(endTime);
              return;
            }
            applyRoomVideoSyncToPlayer({ forceSeek: true });
          }
          // Leader sync is sent only from explicit UI controls + heartbeat.
          // Avoid broadcasting browser-generated state changes on tab hide/show.
        },
        onError: () => {
          showToast(t("videoSyncFailed"));
        }
      }
    });
    activeYouTubeVideoId = videoId;
    return;
  }
  if (activeYouTubeVideoId !== videoId) {
    activeYouTubeVideoId = videoId;
    if (typeof youTubePlayer.loadVideoById === "function") {
      youTubePlayer.loadVideoById(videoId);
    }
  } else if (autoplay) {
    if (typeof youTubePlayer.playVideo === "function") {
      ensureMutedAutoplayForYouTube(youTubePlayer);
      youTubePlayer.playVideo();
    }
  } else if (typeof youTubePlayer.pauseVideo === "function") {
    youTubePlayer.pauseVideo();
  }
  if (autoplay) {
    youTubeAudioUnlockNeeded = true;
    tryUnlockYouTubeAudioFromGesture();
  }
  hardenYouTubeIframeUi();
  startYouTubeTick();
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
  if (isYouTubeRoomVideo(roomVideoState)) {
    const ytDuration = getYouTubeDuration();
    if (ytDuration > 0) {
      return ytDuration;
    }
  }
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
  if (!ROOM_VIDEO_CONTROLS_AUTO_HIDE || !roomVideoState) {
    return false;
  }
  if (roomVideoSeekDragging) {
    return false;
  }
  if (isYouTubeRoomVideo(roomVideoState)) {
    return isYouTubePlaying();
  }
  if (!roomVideoPlayer) {
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

function stopRoomVideoProgressAnimation() {
  if (!roomVideoProgressAnimFrame || typeof cancelAnimationFrame !== "function") {
    roomVideoProgressAnimFrame = 0;
    return;
  }
  cancelAnimationFrame(roomVideoProgressAnimFrame);
  roomVideoProgressAnimFrame = 0;
}

function shouldAnimateRoomVideoProgress() {
  if (!roomVideoState || roomVideoSeekDragging || document.visibilityState === "hidden") {
    return false;
  }
  if (isYouTubeRoomVideo(roomVideoState)) {
    return Boolean(youTubePlayerReady && isYouTubePlaying());
  }
  return Boolean(roomVideoPlayer && !roomVideoPlayer.paused && !roomVideoPlayer.ended);
}

function scheduleRoomVideoProgressAnimation() {
  if (roomVideoProgressAnimFrame || typeof requestAnimationFrame !== "function") {
    return;
  }
  roomVideoProgressAnimFrame = requestAnimationFrame(() => {
    roomVideoProgressAnimFrame = 0;
    if (!shouldAnimateRoomVideoProgress()) {
      return;
    }
    updateRoomVideoControls();
  });
}

function updateRoomVideoControls({ previewTime = null } = {}) {
  const hasVideo = Boolean(roomVideoState && roomVideoState.src);
  const isYoutube = hasVideo && isYouTubeRoomVideo(roomVideoState);
  const canUseCustomControls = hasVideo && (!isYoutube || youTubePlayerReady);
  const canControlTimeline = canUseCustomControls && isRoomLeader();
  const canControlPlayback = canUseCustomControls && isRoomLeader();
  const duration = canUseCustomControls ? getRoomVideoDuration() : 0;
  let currentTime = 0;
  if (canUseCustomControls) {
    currentTime = isYoutube
      ? getSafeYouTubeCurrentTime(duration, { keepMonotonicWhilePlaying: true })
      : getSafeLocalVideoCurrentTime(duration);
  }
  if (previewTime !== null && Number.isFinite(previewTime)) {
    currentTime = clampVideoTime(previewTime, duration);
  }

  if (videoControlsBar) {
    videoControlsBar.classList.toggle("is-disabled", !canUseCustomControls);
  }

  if (videoSeekRange) {
    if (!roomVideoSeekDragging || previewTime !== null || !hasVideo) {
      const progress = duration > 0 ? Math.round((currentTime / duration) * ROOM_VIDEO_SEEK_RANGE_MAX) : 0;
      videoSeekRange.value = String(Math.max(0, Math.min(ROOM_VIDEO_SEEK_RANGE_MAX, progress)));
    }
    const progressPercent = duration > 0 ? Math.max(0, Math.min(100, (currentTime / duration) * 100)) : 0;
    videoSeekRange.style.setProperty("--video-progress", `${progressPercent}%`);
    videoSeekRange.disabled = !canControlTimeline || duration <= 0;
  }

  if (videoTimeText) {
    videoTimeText.textContent = `${formatVideoClock(currentTime)} / ${formatVideoClock(duration)}`;
  }

  if (videoPlayPauseBtn) {
    const isPlaying = canUseCustomControls && (isYoutube ? isYouTubePlaying() : (roomVideoPlayer && !roomVideoPlayer.paused && !roomVideoPlayer.ended));
    videoPlayPauseBtn.disabled = !canControlPlayback;
    videoPlayPauseBtn.textContent = isPlaying ? "❚❚" : "▶";
    videoPlayPauseBtn.setAttribute("aria-label", canControlPlayback ? (isPlaying ? t("videoPauseBtn") : t("videoPlayBtn")) : t("videoHostOnly"));
    videoPlayPauseBtn.title = canControlPlayback ? (isPlaying ? t("videoPauseBtn") : t("videoPlayBtn")) : t("videoHostOnly");
  }

  if (videoFullscreenBtn) {
    videoFullscreenBtn.disabled = !hasVideo;
    const isFullscreen = isVideoFrameFullscreen();
    videoFullscreenBtn.textContent = isFullscreen ? "⤢" : "⛶";
    videoFullscreenBtn.setAttribute("aria-label", isFullscreen ? t("videoExitFullscreenBtn") : t("videoFullscreenBtn"));
    videoFullscreenBtn.title = isFullscreen ? t("videoExitFullscreenBtn") : t("videoFullscreenBtn");
  }
  updateRoomVideoVolumeUi();

  if (!canUseCustomControls || !shouldAutoHideRoomVideoControls()) {
    clearRoomVideoControlsHideTimer();
    setRoomVideoControlsVisible(true);
  }
  if (shouldAnimateRoomVideoProgress()) {
    scheduleRoomVideoProgressAnimation();
  } else {
    stopRoomVideoProgressAnimation();
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

function clampRoomVideoVolume(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return ROOM_VIDEO_DEFAULT_VOLUME;
  }
  return Math.max(0, Math.min(1, numeric));
}

function getEffectiveRoomVideoVolume() {
  return roomVideoLocalMuted ? 0 : clampRoomVideoVolume(roomVideoLocalVolume);
}

function loadRoomVideoVolumePreference() {
  try {
    const raw = localStorage.getItem(ROOM_VIDEO_VOLUME_KEY);
    if (!raw) {
      return;
    }
    const parsed = JSON.parse(raw);
    const savedVolume = clampRoomVideoVolume(parsed?.volume);
    const savedMuted = Boolean(parsed?.muted);
    roomVideoLocalVolume = savedVolume;
    roomVideoLocalMuted = savedMuted || savedVolume <= 0.001;
    if (savedVolume > 0.001) {
      roomVideoVolumeBeforeMute = savedVolume;
    }
  } catch (_error) {
    // Ignore invalid local preference payloads.
  }
}

function saveRoomVideoVolumePreference() {
  try {
    localStorage.setItem(
      ROOM_VIDEO_VOLUME_KEY,
      JSON.stringify({
        volume: clampRoomVideoVolume(roomVideoLocalVolume),
        muted: Boolean(roomVideoLocalMuted)
      })
    );
  } catch (_error) {
    // Ignore storage quota/private mode failures.
  }
}

function applyLocalRoomVideoVolumeToPlayers() {
  const volume = getEffectiveRoomVideoVolume();
  if (roomVideoPlayer) {
    roomVideoPlayer.volume = volume;
    roomVideoPlayer.muted = volume <= 0.001;
  }
  if (youTubePlayer && youTubePlayerReady) {
    const volumePercent = Math.round(volume * 100);
    try {
      if (typeof youTubePlayer.setVolume === "function") {
        youTubePlayer.setVolume(volumePercent);
      }
      if (volumePercent <= 0) {
        if (typeof youTubePlayer.mute === "function") {
          youTubePlayer.mute();
        }
      } else if (roomVideoUserInteracted && typeof youTubePlayer.unMute === "function") {
        youTubePlayer.unMute();
      }
    } catch (_error) {
      // Ignore YouTube volume API failures.
    }
  }
}

function updateRoomVideoVolumeUi() {
  if (!videoVolumeBtn || !videoVolumeRange || !videoVolumeWrap) {
    return;
  }
  const hasVideo = Boolean(roomVideoState && roomVideoState.src);
  const effectiveVolume = getEffectiveRoomVideoVolume();
  const percent = Math.round(effectiveVolume * 100);
  videoVolumeRange.value = String(percent);
  videoVolumeRange.style.setProperty("--video-volume-progress", `${percent}%`);
  videoVolumeBtn.textContent = percent <= 0 ? "🔇" : (percent < 50 ? "🔉" : "🔊");
  const volumeActionLabel = percent <= 0 ? t("videoUnmuteBtn") : t("videoMuteBtn");
  videoVolumeBtn.setAttribute("aria-label", volumeActionLabel);
  videoVolumeBtn.title = volumeActionLabel;
  videoVolumeRange.setAttribute("aria-label", t("videoVolumeLabel"));
  videoVolumeWrap.setAttribute("aria-label", t("videoVolumeLabel"));
  videoVolumeBtn.disabled = !hasVideo;
  videoVolumeRange.disabled = !hasVideo;
}

function setRoomVideoVolume(nextVolume, { mute = null, persist = true } = {}) {
  const clamped = clampRoomVideoVolume(nextVolume);
  const nextMuted = mute === null ? clamped <= 0.001 : Boolean(mute);
  roomVideoLocalVolume = clamped;
  roomVideoLocalMuted = nextMuted;
  if (clamped > 0.001) {
    roomVideoVolumeBeforeMute = clamped;
  }
  applyLocalRoomVideoVolumeToPlayers();
  updateRoomVideoVolumeUi();
  if (persist) {
    saveRoomVideoVolumePreference();
  }
}

function clampVideoTime(value, duration = 0) {
  const numeric = Number(value);
  const safe = Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
  if (duration > 0) {
    return Math.min(duration, safe);
  }
  return safe;
}

function getSafeLocalVideoCurrentTime(duration = 0) {
  const raw = Number(roomVideoPlayer?.currentTime);
  const switchedRecently = Date.now() - Number(roomVideoSourceChangedAt || 0) < ROOM_VIDEO_SOURCE_SWITCH_GUARD_MS;
  const syncTarget = roomVideoSyncState
    ? computeRoomVideoTargetTime(roomVideoSyncState, duration)
    : 0;
  if (Number.isFinite(raw) && raw >= 0) {
    if (
      switchedRecently &&
      raw > 0.25 &&
      syncTarget <= 2 &&
      raw > syncTarget + 2
    ) {
      roomVideoLastKnownTime = clampVideoTime(syncTarget, duration);
      return roomVideoLastKnownTime;
    }
    const clamped = clampVideoTime(raw, duration);
    const known = clampVideoTime(roomVideoLastKnownTime, duration);
    if (
      clamped <= 0.05 &&
      known > 0.25 &&
      !roomVideoPlayer?.ended &&
      !switchedRecently
    ) {
      return known;
    }
    roomVideoLastKnownTime = clamped;
    return clamped;
  }
  const knownFallback = clampVideoTime(roomVideoLastKnownTime, duration);
  if (knownFallback > 0) {
    return knownFallback;
  }
  const syncFallback = roomVideoSyncState
    ? computeRoomVideoTargetTime(roomVideoSyncState, duration)
    : 0;
  const fallback = clampVideoTime(syncFallback, duration);
  roomVideoLastKnownTime = fallback;
  return fallback;
}

function getSafeYouTubeCurrentTime(duration = 0, { keepMonotonicWhilePlaying = false } = {}) {
  const current = clampVideoTime(getYouTubeCurrentTime(), duration);
  const known = clampVideoTime(roomVideoLastKnownTime, duration);
  const rewindWindowActive = Date.now() < roomVideoAllowRewindUntil;

  if (current > 0.05) {
    // Guard against transient YouTube API regressions while playback is active.
    if (keepMonotonicWhilePlaying && isYouTubePlaying()) {
      // During intentional seek/rewind, keep UI on the target instead of stale API readings.
      if (rewindWindowActive && known > 0.05 && Math.abs(current - known) > 0.75) {
        return known;
      }
      if (!rewindWindowActive && current + 0.35 < known) {
        return known;
      }
    }
    roomVideoLastKnownTime = current;
    return current;
  }

  if (known > 0.25 && !youTubeInitialSyncPending) {
    return known;
  }

  const syncFallback = roomVideoSyncState
    ? computeRoomVideoTargetTime(roomVideoSyncState, duration)
    : 0;
  const fallback = clampVideoTime(syncFallback, duration);
  if (fallback > 0) {
    roomVideoLastKnownTime = fallback;
  }
  return fallback;
}

function normalizeIncomingRoomVideoSync(sync, duration = 0) {
  if (!sync) {
    return null;
  }
  const normalizeOrderingValue = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      return 0;
    }
    return Math.floor(numeric);
  };
  const updatedAt = normalizeOrderingValue(sync.updatedAt);
  return {
    ...sync,
    currentTime: clampVideoTime(sync.currentTime, duration),
    playbackRate: normalizeVideoRate(sync.playbackRate),
    updatedAt,
    clientNow: normalizeOrderingValue(sync.clientNow),
    clientSeq: normalizeOrderingValue(sync.clientSeq),
    snapshotAt: Date.now()
  };
}

function compareRoomVideoSyncOrder(incoming, current) {
  const incomingClientNow = Number(incoming?.clientNow || 0);
  const currentClientNow = Number(current?.clientNow || 0);
  if (incomingClientNow > 0 || currentClientNow > 0) {
    if (incomingClientNow !== currentClientNow) {
      return incomingClientNow - currentClientNow;
    }
    const incomingClientSeq = Number(incoming?.clientSeq || 0);
    const currentClientSeq = Number(current?.clientSeq || 0);
    if (incomingClientSeq !== currentClientSeq) {
      return incomingClientSeq - currentClientSeq;
    }
  }
  const incomingUpdatedAt = Number(incoming?.updatedAt || 0);
  const currentUpdatedAt = Number(current?.updatedAt || 0);
  return incomingUpdatedAt - currentUpdatedAt;
}

function shouldAcceptIncomingRoomVideoSync(incoming, current, duration = 0) {
  if (!incoming) {
    return false;
  }
  if (!current) {
    return true;
  }

  const order = compareRoomVideoSyncOrder(incoming, current);
  if (order > 0) {
    return true;
  }
  if (order < 0) {
    return false;
  }

  // Same sync version: ignore stale snapshots that would rewind the timeline.
  const incomingPlaying = Boolean(incoming.playing);
  const currentProjected = computeRoomVideoTargetTime(current, duration);
  if (incomingPlaying) {
    return incoming.currentTime + ROOM_VIDEO_SYNC_STALE_REWIND_SEC >= currentProjected;
  }
  const currentPaused = clampVideoTime(current.currentTime, duration);
  return incoming.currentTime + ROOM_VIDEO_SYNC_STALE_PAUSE_REWIND_SEC >= currentPaused;
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
    }, ROOM_VIDEO_EVENT_SUPPRESS_MS);
  }
}

function isYouTubeRoomVideo(video = roomVideoState) {
  const sourceType = String(video?.sourceType || "").trim().toLowerCase();
  if (sourceType === "youtube") {
    return true;
  }
  if (sourceType === "file") {
    return false;
  }
  const src = String(video?.src || "");
  return Boolean(parseYouTubeVideoId(src));
}

function shouldSendRoomVideoSyncHeartbeat() {
  if (!isRoomLeader() || !roomVideoState || !activeRoomVideoId || !roomVideoPlayer) {
    return false;
  }
  if (roomVideoPageLeaving || document.visibilityState === "hidden") {
    return false;
  }
  if (suppressRoomVideoEvents || roomVideoSeekDragging) {
    return false;
  }
  if (isYouTubeRoomVideo(roomVideoState)) {
    return false;
  }
  if (roomVideoPlayer.ended || roomVideoPlayer.paused) {
    return false;
  }
  return roomVideoPlayer.readyState >= 2;
}

function stopRoomVideoSyncHeartbeat() {
  if (!roomVideoSyncHeartbeatTimer) {
    return;
  }
  clearInterval(roomVideoSyncHeartbeatTimer);
  roomVideoSyncHeartbeatTimer = null;
}

function ensureRoomVideoSyncHeartbeat() {
  if (roomVideoSyncHeartbeatTimer) {
    return;
  }
  roomVideoSyncHeartbeatTimer = setInterval(() => {
    if (!shouldSendRoomVideoSyncHeartbeat()) {
      return;
    }
    sendRoomVideoSync("seek", {
      silent: true,
      includeDuration: false,
      quietError: true,
      heartbeat: true
    });
  }, ROOM_VIDEO_SYNC_HEARTBEAT_MS);
}

function clearRoomVideoUploadHideTimer() {
  if (!roomVideoUploadHideTimer) {
    return;
  }
  clearTimeout(roomVideoUploadHideTimer);
  roomVideoUploadHideTimer = null;
}

function ensureRoomVideoUploadOverlay() {
  if (roomVideoUploadOverlay) {
    return roomVideoUploadOverlay;
  }
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay room-video-upload-overlay hidden";
  overlay.setAttribute("aria-hidden", "true");
  overlay.setAttribute("role", "dialog");
  overlay.setAttribute("aria-modal", "true");

  const card = document.createElement("div");
  card.className = "modal-card room-video-upload-card";

  roomVideoUploadTitleEl = document.createElement("h3");
  roomVideoUploadTitleEl.className = "room-video-upload-title";

  roomVideoUploadHintEl = document.createElement("p");
  roomVideoUploadHintEl.className = "muted room-video-upload-hint";

  const meta = document.createElement("div");
  meta.className = "room-video-upload-meta";

  const createMetaRow = () => {
    const row = document.createElement("div");
    row.className = "room-video-upload-row";
    const label = document.createElement("span");
    label.className = "room-video-upload-label";
    const value = document.createElement("span");
    value.className = "room-video-upload-value";
    row.appendChild(label);
    row.appendChild(value);
    meta.appendChild(row);
    return { label, value };
  };

  const fileRow = createMetaRow();
  roomVideoUploadFileLabelEl = fileRow.label;
  roomVideoUploadFileValueEl = fileRow.value;

  const progressRow = createMetaRow();
  roomVideoUploadProgressLabelEl = progressRow.label;
  roomVideoUploadProgressValueEl = progressRow.value;
  roomVideoUploadProgressValueEl.classList.add("room-video-upload-value-progress");

  const percentRow = createMetaRow();
  roomVideoUploadPercentLabelEl = percentRow.label;
  roomVideoUploadPercentValueEl = percentRow.value;
  roomVideoUploadPercentValueEl.classList.add("room-video-upload-value-percent");

  const track = document.createElement("div");
  track.className = "room-video-upload-track";
  roomVideoUploadBarFillEl = document.createElement("span");
  roomVideoUploadBarFillEl.className = "room-video-upload-fill";
  track.appendChild(roomVideoUploadBarFillEl);

  roomVideoUploadStatusEl = document.createElement("p");
  roomVideoUploadStatusEl.className = "room-video-upload-status";

  card.appendChild(roomVideoUploadTitleEl);
  card.appendChild(roomVideoUploadHintEl);
  card.appendChild(meta);
  card.appendChild(track);
  card.appendChild(roomVideoUploadStatusEl);
  overlay.appendChild(card);

  document.body.appendChild(overlay);
  roomVideoUploadOverlay = overlay;
  return overlay;
}

function clampUploadPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.max(0, Math.min(100, numeric));
}

function formatUploadMbValue(bytes) {
  const safeBytes = Math.max(0, Number(bytes) || 0);
  const mb = safeBytes / (1024 * 1024);
  const fractionDigits = mb >= 100 ? 0 : mb >= 10 ? 1 : 2;
  try {
    return new Intl.NumberFormat(getLang() === "ar" ? "ar" : "en-US", {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits
    }).format(mb);
  } catch (_error) {
    return mb.toFixed(fractionDigits);
  }
}

function updateRoomVideoUploadOverlayText() {
  if (!roomVideoUploadOverlay) {
    return;
  }
  if (roomVideoUploadTitleEl) {
    roomVideoUploadTitleEl.textContent = t("videoUploadModalTitle");
  }
  if (roomVideoUploadHintEl) {
    roomVideoUploadHintEl.textContent = t("videoUploadModalHint");
  }
  if (roomVideoUploadFileLabelEl) {
    roomVideoUploadFileLabelEl.textContent = t("videoUploadModalFileLabel");
  }
  if (roomVideoUploadFileValueEl) {
    roomVideoUploadFileValueEl.textContent = roomVideoUploadState.fileName || "video";
  }
  if (roomVideoUploadProgressLabelEl) {
    roomVideoUploadProgressLabelEl.textContent = t("videoUploadModalProgressLabel");
  }
  const loadedBytes = Math.max(0, Number(roomVideoUploadState.loadedBytes) || 0);
  const totalBytes = Math.max(0, Number(roomVideoUploadState.totalBytes) || 0);
  const loadedMb = formatUploadMbValue(loadedBytes);
  const totalMb = formatUploadMbValue(totalBytes);
  if (roomVideoUploadProgressValueEl) {
    roomVideoUploadProgressValueEl.textContent = totalBytes > 0
      ? fmt(t("videoUploadModalProgressValue"), {
          loaded: loadedMb,
          total: totalMb
        })
      : fmt(t("videoUploadModalProgressUnknown"), {
          loaded: loadedMb
        });
  }
  const percent = clampUploadPercent(roomVideoUploadState.percent);
  if (roomVideoUploadPercentLabelEl) {
    roomVideoUploadPercentLabelEl.textContent = t("videoUploadModalPercentLabel");
  }
  if (roomVideoUploadPercentValueEl) {
    roomVideoUploadPercentValueEl.textContent = `${Math.round(percent)}%`;
  }
  if (roomVideoUploadBarFillEl) {
    roomVideoUploadBarFillEl.style.width = `${percent}%`;
  }
  if (roomVideoUploadStatusEl) {
    let statusKey = "videoUploadModalPreparing";
    if (roomVideoUploadState.status === "uploading") {
      statusKey = "videoUploadModalUploading";
    } else if (roomVideoUploadState.status === "processing") {
      statusKey = "videoUploadModalProcessing";
    } else if (roomVideoUploadState.status === "done") {
      statusKey = "videoUploadModalDone";
    }
    roomVideoUploadStatusEl.textContent = t(statusKey);
  }
}

function showRoomVideoUploadOverlay(file) {
  if (!isRoomLeader()) {
    return;
  }
  clearRoomVideoUploadHideTimer();
  const overlay = ensureRoomVideoUploadOverlay();
  roomVideoUploadState = {
    fileName: String(file?.name || "video"),
    totalBytes: Math.max(0, Number(file?.size) || 0),
    loadedBytes: 0,
    percent: 0,
    status: "preparing"
  };
  updateRoomVideoUploadOverlayText();
  overlay.classList.remove("hidden");
  overlay.setAttribute("aria-hidden", "false");
  document.body.classList.add("room-video-upload-lock");
}

function setRoomVideoUploadProgress({
  loadedBytes = null,
  totalBytes = null,
  percent = null,
  status = null
} = {}) {
  if (!roomVideoUploadOverlay || roomVideoUploadOverlay.classList.contains("hidden")) {
    return;
  }
  if (typeof status === "string" && status) {
    roomVideoUploadState.status = status;
  }
  const nextTotal = Number(totalBytes);
  if (Number.isFinite(nextTotal) && nextTotal >= 0) {
    roomVideoUploadState.totalBytes = nextTotal;
  }
  const nextLoaded = Number(loadedBytes);
  if (Number.isFinite(nextLoaded) && nextLoaded >= 0) {
    roomVideoUploadState.loadedBytes = nextLoaded;
  }
  if (roomVideoUploadState.totalBytes > 0 && roomVideoUploadState.loadedBytes > roomVideoUploadState.totalBytes) {
    roomVideoUploadState.loadedBytes = roomVideoUploadState.totalBytes;
  }
  const nextPercent = Number(percent);
  if (Number.isFinite(nextPercent)) {
    roomVideoUploadState.percent = clampUploadPercent(nextPercent);
  } else if (roomVideoUploadState.totalBytes > 0) {
    roomVideoUploadState.percent = clampUploadPercent(
      (roomVideoUploadState.loadedBytes / roomVideoUploadState.totalBytes) * 100
    );
  }
  if (roomVideoUploadState.status === "done") {
    roomVideoUploadState.percent = 100;
  }
  updateRoomVideoUploadOverlayText();
}

function hideRoomVideoUploadOverlay({ delayMs = 0 } = {}) {
  clearRoomVideoUploadHideTimer();
  const close = () => {
    if (!roomVideoUploadOverlay) {
      document.body.classList.remove("room-video-upload-lock");
      return;
    }
    roomVideoUploadOverlay.classList.add("hidden");
    roomVideoUploadOverlay.setAttribute("aria-hidden", "true");
    document.body.classList.remove("room-video-upload-lock");
    roomVideoUploadState = {
      fileName: "",
      totalBytes: 0,
      loadedBytes: 0,
      percent: 0,
      status: "idle"
    };
  };
  if (delayMs > 0) {
    roomVideoUploadHideTimer = setTimeout(close, delayMs);
    return;
  }
  close();
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
  if (videoYoutubeBtn) {
    videoYoutubeBtn.disabled = isUploadingRoomVideo;
    videoYoutubeBtn.textContent = isUploadingRoomVideo ? t("videoYoutubeBusy") : t("videoYoutubeBtn");
  }
  if (videoYoutubeInput) {
    videoYoutubeInput.disabled = isUploadingRoomVideo;
  }
  updateVideoClearButtonState();
  updateRoomVideoQueueUi();
}

function normalizeRoomVideoQueueItems(queue) {
  if (!Array.isArray(queue)) {
    return [];
  }
  return queue
    .filter((item) => item && typeof item === "object" && String(item.src || "").trim())
    .map((item, index) => ({
      ...item,
      queueIndex: Number.isInteger(item.queueIndex) ? item.queueIndex : index
    }));
}

function syncRoomVideoQueueFromPayload(payload) {
  if (payload && typeof payload === "object") {
    if (Object.prototype.hasOwnProperty.call(payload, "videoQueue")) {
      roomVideoQueueState = normalizeRoomVideoQueueItems(payload.videoQueue);
    } else if (Object.prototype.hasOwnProperty.call(payload, "queue")) {
      roomVideoQueueState = normalizeRoomVideoQueueItems(payload.queue);
    }
  }
  if (roomVideoQueueState.length === 0) {
    cancelRoomVideoAutoNextCountdown();
  }
  updateRoomVideoQueueUi();
}

function updateRoomVideoQueueUi() {
  if (!videoQueueTools) {
    return;
  }
  videoQueueTools.classList.remove("hidden");
  const isLeader = isRoomLeader();
  const queueSize = roomVideoQueueState.length;
  if (queueSize === 0 && roomVideoAutoNextVideoId) {
    cancelRoomVideoAutoNextCountdown();
  }
  const busy = isUploadingRoomVideo || isManagingRoomVideoQueue;
  const canManage = isLeader && !busy;
  const canGoNext = canManage && queueSize > 0;
  if (videoQueueNextBtn) {
    videoQueueNextBtn.classList.toggle("hidden", !isLeader);
    videoQueueNextBtn.disabled = !canGoNext;
  }
  if (videoQueueList) {
    videoQueueList.innerHTML = "";
    roomVideoQueueState.forEach((item, index) => {
      const li = document.createElement("li");
      li.className = "video-queue-item";

      const info = document.createElement("div");
      info.className = "video-queue-item-info";

      const order = document.createElement("span");
      order.className = "video-queue-item-order";
      order.textContent = `${index + 1}.`;
      info.appendChild(order);

      const title = document.createElement("span");
      title.className = "video-queue-item-title";
      const fileName = String(item.filename || "video");
      const uploader = videoUploaderName(item.uploadedBy);
      title.textContent = uploader
        ? `${fileName} ${fmt(t("videoByUserInline"), { user: uploader })}`
        : fileName;
      info.appendChild(title);

      li.appendChild(info);
      if (isLeader) {
        const removeBtn = document.createElement("button");
        removeBtn.type = "button";
        removeBtn.className = "btn btn-ghost video-queue-item-remove";
        removeBtn.textContent = "X";
        removeBtn.disabled = busy;
        removeBtn.title = t("videoQueueRemoveItem");
        removeBtn.setAttribute("aria-label", t("videoQueueRemoveItem"));
        removeBtn.addEventListener("click", () => {
          sfx("click");
          removeRoomVideoQueueItem(Number(item.queueIndex ?? index));
        });
        li.appendChild(removeBtn);
      }
      videoQueueList.appendChild(li);
    });
  }
  if (videoQueueEmpty) {
    videoQueueEmpty.classList.toggle("hidden", queueSize > 0);
    videoQueueEmpty.textContent = t("videoQueueEmpty");
  }
}

function setRoomVideoQueueBusy(busy) {
  isManagingRoomVideoQueue = Boolean(busy);
  updateRoomVideoQueueUi();
}

function updateRoomVideoAutoNextCountdownText(seconds) {
  if (!videoQueueCountdownNotice) {
    return;
  }
  const safeSeconds = Math.max(1, Math.floor(Number(seconds) || 0));
  videoQueueCountdownNotice.textContent = fmt(t("videoQueueAutoNextCountdown"), {
    seconds: safeSeconds
  });
  videoQueueCountdownNotice.classList.remove("hidden");
}

function clearRoomVideoAutoNextCountdownTimer() {
  if (!roomVideoAutoNextCountdownTimer) {
    return;
  }
  clearInterval(roomVideoAutoNextCountdownTimer);
  roomVideoAutoNextCountdownTimer = null;
}

function cancelRoomVideoAutoNextCountdown() {
  clearRoomVideoAutoNextCountdownTimer();
  roomVideoAutoNextCountdownSeconds = 0;
  roomVideoAutoNextVideoId = "";
  if (videoQueueCountdownNotice) {
    videoQueueCountdownNotice.classList.add("hidden");
    videoQueueCountdownNotice.textContent = "";
  }
}

function startRoomVideoAutoNextCountdown(videoId) {
  const targetVideoId = String(videoId || "").trim();
  if (!targetVideoId || roomVideoQueueState.length === 0) {
    return false;
  }
  const sameCountdown =
    roomVideoAutoNextVideoId === targetVideoId &&
    (roomVideoAutoNextCountdownTimer || roomVideoAutoNextCountdownSeconds > 0);
  if (sameCountdown) {
    return true;
  }
  cancelRoomVideoAutoNextCountdown();
  roomVideoAutoNextVideoId = targetVideoId;
  roomVideoAutoNextCountdownSeconds = ROOM_VIDEO_AUTO_NEXT_DELAY_SEC;
  updateRoomVideoAutoNextCountdownText(roomVideoAutoNextCountdownSeconds);

  roomVideoAutoNextCountdownTimer = setInterval(() => {
    roomVideoAutoNextCountdownSeconds = Math.max(0, roomVideoAutoNextCountdownSeconds - 1);
    if (roomVideoAutoNextCountdownSeconds > 0) {
      updateRoomVideoAutoNextCountdownText(roomVideoAutoNextCountdownSeconds);
      return;
    }
    clearRoomVideoAutoNextCountdownTimer();
    if (!isRoomLeader()) {
      cancelRoomVideoAutoNextCountdown();
    }
  }, 1000);
  return true;
}

function markRoomVideoSourceChanged() {
  roomVideoSourceChangedAt = Date.now();
  roomVideoAllowRewindUntil = 0;
}

function allowRoomVideoRewindTemporarily(ms = ROOM_VIDEO_REWIND_ALLOW_MS) {
  const ttl = Number(ms);
  if (!Number.isFinite(ttl) || ttl <= 0) {
    return;
  }
  roomVideoAllowRewindUntil = Math.max(roomVideoAllowRewindUntil, Date.now() + ttl);
}

function resetRoomVideoProgressUi() {
  roomVideoSeekDragging = false;
  roomVideoLastKnownTime = 0;
  roomVideoAllowRewindUntil = 0;
  if (videoSeekRange) {
    videoSeekRange.value = "0";
    videoSeekRange.style.setProperty("--video-progress", "0%");
  }
  if (videoTimeText) {
    videoTimeText.textContent = `${formatVideoClock(0)} / ${formatVideoClock(0)}`;
  }
}

function updateVideoClearButtonState() {
  const hasVideo = Boolean(roomVideoState && roomVideoState.src);
  const isLeader = isRoomLeader();
  const mobile = isRoomMobileLayout();
  const canClear = isLeader && hasVideo && !isUploadingRoomVideo;
  let label = t("videoClearBtn");
  if (!isLeader) {
    label = t("videoHostOnly");
  } else if (!hasVideo) {
    label = t("videoNoVideo");
  }
  const updateButton = (button, visible) => {
    if (!button) {
      return;
    }
    button.classList.toggle("hidden", !visible);
    button.disabled = !canClear;
    button.textContent = t("videoClearBtn");
    button.setAttribute("aria-label", label);
    button.title = label;
  };
  updateButton(videoClearTopBtn, isLeader && !mobile);
  updateButton(videoClearBtn, isLeader);
}

function updateVideoEmptyNoticeState() {
  if (!videoEmptyNotice) {
    return;
  }
  const hasVideo = Boolean(roomVideoState && roomVideoState.src);
  videoEmptyNotice.classList.toggle("hidden", hasVideo);
}

function showVideoControlLockedToast() {
  // Hidden by request: members should not see lock toast.
}

function clearRoomVideoPlayer() {
  if (!roomVideoPlayer) {
    return;
  }
  stopRoomVideoSyncHeartbeat();
  cancelRoomVideoAutoNextCountdown();
  markRoomVideoSourceChanged();
  resetRoomVideoProgressUi();
  clearRoomVideoControlsHideTimer();
  destroyYouTubePlayer();
  withSuppressedRoomVideoEvents(() => {
    roomVideoPlayer.pause();
    roomVideoPlayer.removeAttribute("src");
    roomVideoPlayer.load();
  });
  if (roomVideoEmbed) {
    roomVideoEmbed.removeAttribute("src");
    roomVideoEmbed.classList.add("hidden");
  }
  roomVideoPlayer.classList.remove("hidden");
  activeRoomVideoId = "";
  roomVideoMetadataReady = false;
  roomVideoState = null;
  roomVideoSyncState = null;
  roomVideoResumePending = false;
  roomVideoEndTransitionInFlight = false;
  youTubeLeaderPlayPending = false;
  updateVideoEmptyNoticeState();
  setYouTubeMaskVisible(false);
  hideFullscreenChatNotice();
  setRoomVideoControlsVisible(true);
  updateRoomVideoControls();
  updateVideoClearButtonState();
  updateRoomVideoQueueUi();
}

async function applyRoomVideoSyncToPlayer({ forceSeek = false } = {}) {
  if (!roomVideoState || !roomVideoSyncState) {
    return;
  }
  if (!roomVideoSyncState.playing) {
    roomVideoResumePending = false;
  }
  if (isRoomLeader() && !forceSeek && Date.now() < roomVideoLeaderIgnoreSyncUntil) {
    updateRoomVideoControls();
    return;
  }
  if (isYouTubeRoomVideo(roomVideoState)) {
    if (!youTubePlayer || !youTubePlayerReady) {
      return;
    }
    const duration = getYouTubeDuration();
    const targetTime = computeRoomVideoTargetTime(roomVideoSyncState, duration);
    const current = getSafeYouTubeCurrentTime(duration, { keepMonotonicWhilePlaying: true });
    const drift = Math.abs(targetTime - current);
    const leaderControl = isRoomLeader();
    const hardSeekThreshold = leaderControl ? ROOM_VIDEO_LEADER_SEEK_DRIFT_SEC : ROOM_VIDEO_MEMBER_HARD_SEEK_DRIFT_SEC;
    const nextRate = normalizeVideoRate(roomVideoSyncState.playbackRate);

    suppressRoomVideoEvents = true;
    suppressYouTubeStateBroadcast = true;
    try {
      if (typeof youTubePlayer.setPlaybackRate === "function") {
        try {
          youTubePlayer.setPlaybackRate(nextRate);
        } catch (_) {
          // Some videos restrict playback rates.
        }
      }
      if (forceSeek || drift > hardSeekThreshold) {
        if (typeof youTubePlayer.seekTo === "function") {
          allowRoomVideoRewindTemporarily();
          youTubePlayer.seekTo(targetTime, true);
        }
      }
      if (roomVideoSyncState.playing) {
        youTubeAudioUnlockNeeded = true;
        ensureMutedAutoplayForYouTube(youTubePlayer);
        tryUnlockYouTubeAudioFromGesture();
        if (typeof youTubePlayer.playVideo === "function") {
          youTubePlayer.playVideo();
        }
      } else if (typeof youTubePlayer.pauseVideo === "function") {
        youTubePlayer.pauseVideo();
      }
    } finally {
      setTimeout(() => {
        suppressRoomVideoEvents = false;
        suppressYouTubeStateBroadcast = false;
      }, ROOM_VIDEO_EVENT_SUPPRESS_MS);
      roomVideoLastKnownTime = clampVideoTime(targetTime, duration);
      youTubeInitialSyncPending = false;
      setYouTubeMaskVisible(!roomVideoSyncState.playing);
      updateRoomVideoControls();
    }
    return;
  }
  if (!roomVideoPlayer) {
    return;
  }
  suppressRoomVideoEvents = true;
  const duration = Number.isFinite(roomVideoPlayer.duration) && roomVideoPlayer.duration > 0
    ? Number(roomVideoPlayer.duration)
    : Number(roomVideoState.duration || 0);
  const targetTime = computeRoomVideoTargetTime(roomVideoSyncState, duration);
  const baseRate = normalizeVideoRate(roomVideoSyncState.playbackRate);
  const localTime = getSafeLocalVideoCurrentTime(duration);
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
      if (!roomVideoUserInteracted) {
        roomVideoPlayer.muted = true;
      } else {
        applyLocalRoomVideoVolumeToPlayers();
      }
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
    }, ROOM_VIDEO_EVENT_SUPPRESS_MS);
    updateRoomVideoControls();
  }
}

function renderRoomVideo(room) {
  if (!videoLeaderTools || !videoStatusText || !videoHintText || !roomVideoPlayer) {
    return;
  }
  syncRoomVideoQueueFromPayload(room);

  const canControl = canUseVideoTools();
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
    updateVideoClearButtonState();
    updateRoomVideoQueueUi();
    return;
  }

  ensureRoomVideoSyncHeartbeat();
  roomVideoState = nextVideo;
  updateVideoEmptyNoticeState();
  updateVideoClearButtonState();
  updateRoomVideoQueueUi();
  const incomingDuration = Number(nextVideo.duration || 0);
  const incomingSyncState = normalizeIncomingRoomVideoSync(nextVideo.sync, incomingDuration);
  const nextVideoId = String(nextVideo.id || "");
  const isYoutube = isYouTubeRoomVideo(nextVideo);
  const sourceChanged = activeRoomVideoId !== nextVideoId;
  if (sourceChanged) {
    markRoomVideoSourceChanged();
    resetRoomVideoProgressUi();
  }
  if (roomVideoAutoNextVideoId && roomVideoAutoNextVideoId !== nextVideoId) {
    cancelRoomVideoAutoNextCountdown();
  }
  if (roomVideoAutoNextVideoId === nextVideoId && roomVideoQueueState.length === 0) {
    cancelRoomVideoAutoNextCountdown();
  }
  if (sourceChanged || !incomingSyncState) {
    roomVideoSyncState = incomingSyncState;
  } else if (shouldAcceptIncomingRoomVideoSync(incomingSyncState, roomVideoSyncState, incomingDuration)) {
    roomVideoSyncState = incomingSyncState;
  }
  if (sourceChanged && roomVideoSyncState) {
    roomVideoSyncState.currentTime = 0;
    roomVideoSyncState.snapshotAt = Date.now();
  }
  activeRoomVideoId = nextVideoId;

  // Always enforce a single visible renderer to avoid stacked video views.
  roomVideoPlayer.classList.toggle("hidden", isYoutube);
  if (roomVideoEmbed) {
    roomVideoEmbed.classList.toggle("hidden", !isYoutube);
  }
  if (!isYoutube) {
    setYouTubeMaskVisible(false);
  }

  if (isYoutube) {
    roomVideoMetadataReady = false;
    if (sourceChanged) {
      youTubeLeaderPlayPending = false;
    }
    const shouldAutoplay = roomVideoSyncState ? Boolean(roomVideoSyncState.playing) : true;
    youTubeAudioUnlockNeeded = shouldAutoplay;
    const youtubeId = parseYouTubeVideoId(nextVideo.youtubeId) || parseYouTubeVideoId(nextVideo.src);
    const shouldResetNativePlayer =
      sourceChanged ||
      !youTubePlayer ||
      !youTubePlayerReady ||
      activeYouTubeVideoId !== youtubeId;
    youTubeInitialSyncPending = Boolean(roomVideoSyncState && shouldResetNativePlayer);
    if (shouldResetNativePlayer) {
      withSuppressedRoomVideoEvents(() => {
        roomVideoPlayer.pause();
        roomVideoPlayer.removeAttribute("src");
        roomVideoPlayer.load();
      });
      ensureYouTubePlayer(youtubeId, { autoplay: shouldAutoplay }).catch(() => {
        showToast(t("videoSyncFailed"));
      });
    }
  } else if (sourceChanged) {
    destroyYouTubePlayer();
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
  const uploader = videoUploaderName(nextVideo.uploadedBy);
  if (uploader) {
    videoStatusText.textContent = fmt(t("videoNowPlayingWithUser"), { name: fileLabel, user: uploader });
  } else {
    videoStatusText.textContent = fmt(t("videoNowPlaying"), { name: fileLabel });
  }
  applyLocalRoomVideoVolumeToPlayers();
  updateRoomVideoControls();
  if (!roomVideoSyncState) {
    return;
  }
  if (isYoutube) {
    applyRoomVideoSyncToPlayer({ forceSeek: sourceChanged });
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

  showRoomVideoUploadOverlay(file);
  setRoomVideoUploadProgress({
    status: "preparing",
    loadedBytes: 0,
    totalBytes: Number(file.size || 0),
    percent: 0
  });

  const duration = await readVideoDurationFromFile(file);
  const formData = new FormData();
  formData.append("video", file, file.name);
  if (duration > 0) {
    formData.append("duration", String(duration));
  }
  formData.append("enqueue", "true");

  setVideoUploadBusy(true);
  let uploadSucceeded = false;
  try {
    const result = await uploadRoomVideoRequest(`/api/rooms/${encodedCode}/video`, formData, {
      onStart: () => {
        setRoomVideoUploadProgress({
          status: "uploading",
          loadedBytes: 0,
          totalBytes: Number(file.size || 0),
          percent: 0
        });
      },
      onProgress: ({ loadedBytes, totalBytes, percent }) => {
        setRoomVideoUploadProgress({
          status: "uploading",
          loadedBytes,
          totalBytes: totalBytes || Number(file.size || 0),
          percent
        });
      },
      onUploadComplete: () => {
        setRoomVideoUploadProgress({
          status: "processing",
          loadedBytes: Number(file.size || 0),
          totalBytes: Number(file.size || 0),
          percent: 100
        });
      }
    });
    setRoomVideoUploadProgress({
      status: "done",
      loadedBytes: Number(file.size || 0),
      totalBytes: Number(file.size || 0),
      percent: 100
    });
    uploadSucceeded = true;
    videoFileInput.value = "";
    const queued = Boolean(result?.queued);
    showToast(queued ? t("videoQueueQueuedUpload") : t("videoUploadSuccess"), "success");
    if (result?.room) {
      renderRoomInfo(result.room);
      if (!queued) {
        triggerAutoPlayForNewRoomVideo();
      }
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
    hideRoomVideoUploadOverlay({ delayMs: uploadSucceeded ? 420 : 0 });
  }
}

async function setRoomYouTubeVideo() {
  const input = String(videoYoutubeInput?.value || "").trim();
  if (input.length < 2) {
    showToast(t("videoYoutubeNeedInput"));
    return;
  }

  setVideoUploadBusy(true);
  try {
    const result = await api(`/api/rooms/${encodedCode}/video-source`, {
      method: "POST",
      body: {
        input,
        enqueue: true
      }
    });
    if (videoYoutubeInput) {
      videoYoutubeInput.value = "";
    }
    const queued = Boolean(result?.queued);
    showToast(queued ? t("videoQueueQueuedYoutube") : t("videoYoutubeSuccess"), "success");
    if (result?.room) {
      renderRoomInfo(result.room);
      if (!queued) {
        triggerAutoPlayForNewRoomVideo();
      }
    }
    await refreshMessages();
  } catch (error) {
    if (error.code === "YOUTUBE_NOT_FOUND") {
      showToast(t("videoYoutubeNotFound"));
      return;
    }
    if (error.code === "YOUTUBE_RESOLVE_FAILED") {
      showToast(t("videoYoutubeResolveFail"));
      return;
    }
    showToast(error.message || t("videoSyncFailed"));
  } finally {
    setVideoUploadBusy(false);
  }
}

async function clearRoomVideo({ silentSuccessToast = false } = {}) {
  if (!isRoomLeader()) {
    showToast(t("videoHostOnly"));
    return;
  }
  if (!roomVideoState || !roomVideoState.src) {
    showToast(t("videoNoVideo"));
    return;
  }

  if (roomVideoQueueState.length > 0) {
    await runRoomVideoQueueAction("next", {}, {
      successToastKey: "videoClearSuccess",
      refreshChat: true
    });
    triggerAutoPlayForNewRoomVideo();
    return;
  }

  setVideoUploadBusy(true);
  try {
    let result = null;
    try {
      result = await api(`/api/rooms/${encodedCode}/video`, {
        method: "DELETE"
      });
    } catch (error) {
      const message = String(error?.message || "");
      const shouldFallback =
        Number(error?.status || 0) === 404
          || message.toLowerCase().includes("endpoint")
          || message.includes("نقطة وصول");
      if (!shouldFallback) {
        throw error;
      }
      result = await api(`/api/rooms/${encodedCode}/video`, {
        method: "POST",
        body: { action: "clear" }
      });
    }
    if (!silentSuccessToast) {
      showToast(t("videoClearSuccess"), "success");
    }
    if (result?.room) {
      renderRoomInfo(result.room);
    }
    await refreshMessages();
  } catch (error) {
    showToast(error.message || t("videoSyncFailed"));
  } finally {
    setVideoUploadBusy(false);
  }
}

async function handleLeaderVideoEndedAutoAdvance(endTime = null) {
  if (!isRoomLeader() || roomVideoEndTransitionInFlight || !roomVideoState) {
    return;
  }
  const endedVideoId = String(activeRoomVideoId || roomVideoState?.id || "").trim();
  if (!endedVideoId || roomVideoQueueState.length === 0) {
    cancelRoomVideoAutoNextCountdown();
    if (endedVideoId && roomVideoQueueState.length === 0) {
      await clearRoomVideo({ silentSuccessToast: true });
    }
    return;
  }
  roomVideoEndTransitionInFlight = true;
  try {
    const safeEndTime = Number(endTime);
    startRoomVideoAutoNextCountdown(endedVideoId);
    await sendRoomVideoSync("pause", {
      silent: true,
      quietError: true,
      allowRewind: true,
      currentTimeOverride: Number.isFinite(safeEndTime) && safeEndTime >= 0 ? safeEndTime : undefined
    });

    await new Promise((resolve) => {
      setTimeout(resolve, ROOM_VIDEO_AUTO_NEXT_DELAY_SEC * 1000);
    });

    if (!isRoomLeader() || !roomVideoState) {
      return;
    }
    const currentVideoId = String(activeRoomVideoId || roomVideoState?.id || "").trim();
    if (!currentVideoId || currentVideoId !== endedVideoId) {
      return;
    }
    if (roomVideoQueueState.length === 0) {
      await clearRoomVideo({ silentSuccessToast: true });
      return;
    }

    const nextResult = await api(`/api/rooms/${encodedCode}/video-queue`, {
      method: "POST",
      body: { action: "next" }
    });
    if (nextResult?.room) {
      renderRoomInfo(nextResult.room);
      triggerAutoPlayForNewRoomVideo();
    } else {
      syncRoomVideoQueueFromPayload(nextResult);
    }
    await refreshMessages();
  } catch (error) {
    if (error.code !== "VIDEO_QUEUE_EMPTY") {
      showToast(error.message || t("videoSyncFailed"));
    }
  } finally {
    cancelRoomVideoAutoNextCountdown();
    setTimeout(() => {
      roomVideoEndTransitionInFlight = false;
    }, 160);
  }
}

function uploadRoomVideoRequest(
  pathname,
  formData,
  {
    onStart = null,
    onProgress = null,
    onUploadComplete = null
  } = {}
) {
  return new Promise((resolve, reject) => {
    const token = getToken();
    const xhr = new XMLHttpRequest();
    let uploadCompleteNotified = false;
    const notifyUploadComplete = () => {
      if (uploadCompleteNotified || typeof onUploadComplete !== "function") {
        return;
      }
      uploadCompleteNotified = true;
      try {
        onUploadComplete();
      } catch (_error) {
        // Ignore callback errors to avoid breaking upload flow.
      }
    };
    xhr.open("POST", pathname, true);
    xhr.responseType = "json";
    xhr.timeout = 0;
    xhr.setRequestHeader("X-Lang", getLang());
    if (token) {
      xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    }
    if (typeof onProgress === "function" && xhr.upload) {
      xhr.upload.onprogress = (event) => {
        const loadedBytes = Math.max(0, Number(event?.loaded) || 0);
        const totalBytes = event?.lengthComputable
          ? Math.max(0, Number(event.total) || 0)
          : 0;
        const percent = totalBytes > 0 ? (loadedBytes / totalBytes) * 100 : null;
        try {
          onProgress({
            loadedBytes,
            totalBytes,
            percent
          });
        } catch (_error) {
          // Ignore callback errors to avoid breaking upload flow.
        }
      };
    }
    if (xhr.upload) {
      xhr.upload.onload = () => {
        notifyUploadComplete();
      };
    }

    xhr.onload = () => {
      notifyUploadComplete();
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

    if (typeof onStart === "function") {
      try {
        onStart();
      } catch (_error) {
        // Ignore callback errors to avoid breaking upload flow.
      }
    }
    xhr.send(formData);
  });
}

async function sendRoomVideoSync(
  action,
  {
    silent = true,
    includeDuration = true,
    quietError = false,
    currentTimeOverride = null,
    heartbeat = false,
    allowRewind = false
  } = {}
) {
  if (!isRoomLeader() || !roomVideoState || !activeRoomVideoId || !roomVideoPlayer) {
    if (!isRoomLeader()) {
      showVideoControlLockedToast();
    }
    return;
  }
  if (isRoomLeader()) {
    roomVideoLeaderIgnoreSyncUntil = Math.max(
      roomVideoLeaderIgnoreSyncUntil,
      Date.now() + ROOM_VIDEO_LEADER_SYNC_SUPPRESS_MS
    );
  }
  try {
    const useYouTube = isYouTubeRoomVideo(roomVideoState);
    const duration = includeDuration
      ? (useYouTube
          ? (getYouTubeDuration() > 0 ? getYouTubeDuration() : undefined)
          : (Number.isFinite(roomVideoPlayer.duration) && roomVideoPlayer.duration > 0 ? Number(roomVideoPlayer.duration) : undefined))
      : undefined;
    const overrideTime = Number(currentTimeOverride);
    const hasOverrideTime = Number.isFinite(overrideTime) && overrideTime >= 0;
    const safeDuration = useYouTube ? getYouTubeDuration() : getRoomVideoDuration();
    let currentTime = hasOverrideTime
      ? overrideTime
      : (useYouTube
          ? getSafeYouTubeCurrentTime(safeDuration, { keepMonotonicWhilePlaying: true })
          : getSafeLocalVideoCurrentTime(safeDuration));
    if (!useYouTube) {
      const known = clampVideoTime(roomVideoLastKnownTime, safeDuration);
      if (!hasOverrideTime && action !== "stop" && currentTime <= 0.05 && known > 0.25) {
        currentTime = known;
      }
      if (currentTime > 0.05) {
        roomVideoLastKnownTime = clampVideoTime(currentTime, safeDuration);
      }
    } else if (currentTime > 0.05) {
      roomVideoLastKnownTime = clampVideoTime(currentTime, safeDuration);
    }
    const rawRate = useYouTube
      ? Number(
          youTubePlayer && youTubePlayerReady && typeof youTubePlayer.getPlaybackRate === "function"
            ? youTubePlayer.getPlaybackRate()
            : 1
        )
      : Number(roomVideoPlayer.playbackRate || 1);
    const playing = useYouTube
      ? isYouTubePlaying()
      : Boolean(roomVideoPlayer && !roomVideoPlayer.paused && !roomVideoPlayer.ended);
    const payload = {
      action,
      videoId: activeRoomVideoId,
      currentTime,
      playing,
      playbackRate: normalizeVideoRate(rawRate),
      duration,
      heartbeat: Boolean(heartbeat),
      allowRewind: Boolean(allowRewind),
      clientNow: Date.now(),
      clientSeq: ++roomVideoSyncClientSeq
    };
    const result = await api(`/api/rooms/${encodedCode}/video-sync`, {
      method: "POST",
      body: payload
    });
    // Avoid re-rendering the full room on each sync action; polling will deliver state updates.
    void result;
  } catch (error) {
    if (!quietError) {
      showToast(error.message || t("videoSyncFailed"));
    }
  }
}

function handleRoomVideoControlEvent(action) {
  if (suppressRoomVideoEvents || !roomVideoState) {
    return;
  }
  if (roomVideoPageLeaving || document.visibilityState === "hidden") {
    return;
  }
  if (!isRoomLeader()) {
    applyRoomVideoSyncToPlayer({ forceSeek: true });
    showVideoControlLockedToast();
    return;
  }
  // File playback sync is driven by explicit UI actions + heartbeat.
  // This avoids broadcasting browser auto-pause events when the tab is backgrounded.
  if (!isYouTubeRoomVideo(roomVideoState)) {
    return;
  }
  sendRoomVideoSync(action, { allowRewind: action === "seek" });
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
  closeRoomSocket({ manual: true });
  stopRoomPolling();
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
  roomVideoPageLeaving = true;
  closeRoomSocket({ manual: true });
  stopRoomPolling();
  if (roomRefreshDebounceTimer) {
    clearTimeout(roomRefreshDebounceTimer);
    roomRefreshDebounceTimer = null;
  }
  setRoomSupervisorOpen(false);
  hideGlobalAnnouncement();
  queuedAnnouncement = null;
  clearReplyTarget();
  closeReactionPickers();
  messageReactionState.clear();
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
    ...(CLIENT_COUNTRY_CODE ? { "X-Country": CLIENT_COUNTRY_CODE } : {}),
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
  if (message.key === "room_video_set") {
    return fmt(t("sysVideoSet"), {
      user: message.payload?.user || "",
      name: message.payload?.name || "video"
    });
  }
  if (message.key === "room_video_removed") {
    return fmt(t("sysVideoRemoved"), { user: message.payload?.user || "" });
  }
  if (message.key === "message_reaction") {
    return "";
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
  if (!joinModalTimer) {
    return;
  }
  if (requestModalSeconds > 0) {
    joinModalTimer.textContent = fmt(t("joinModalTimer"), { seconds: requestModalSeconds });
    return;
  }
  joinModalTimer.textContent = t("joinModalTimer");
}

async function dismissActiveModalRequest(action, silentToast = false) {
  const target = activeRequestUser;
  if (!target) {
    closeRequestModal();
    return;
  }
  dismissedRequestUsers.delete(target);
  closeRequestModal();
  await handleRequestDecision(target, action, { silentToast });
}

function postponeActiveModalRequest() {
  if (activeRequestUser) {
    dismissedRequestUsers.add(activeRequestUser);
  }
  closeRequestModal();
  maybeOpenNextRequestModal();
}

function openRequestModal(username) {
  activeRequestUser = username;
  requestModalSeconds = 0;
  joinModalTitle.textContent = t("joinModalTitle");
  joinModalText.textContent = fmt(t("joinModalPrompt"), { user: username });
  updateModalTimerText();
  sfx("modal");
  joinRequestModal.classList.remove("hidden");
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
    dismissedRequestUsers.clear();
    closeRequestModal();
    return;
  }
  const activeSet = new Set(requests);
  requestModalQueue = requestModalQueue.filter((user) => activeSet.has(user) && !dismissedRequestUsers.has(user));
  Array.from(dismissedRequestUsers).forEach((user) => {
    if (!activeSet.has(user)) {
      dismissedRequestUsers.delete(user);
    }
  });
  if (activeRequestUser && !activeSet.has(activeRequestUser)) {
    closeRequestModal();
  }
  requests.forEach((user) => {
    if (user !== activeRequestUser && !dismissedRequestUsers.has(user) && !requestModalQueue.includes(user)) {
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
    dismissedRequestUsers.delete(username);
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

async function runRoomVideoQueueAction(action, payload = {}, {
  successToastKey = "",
  refreshChat = false
} = {}) {
  if (!isRoomLeader()) {
    showToast(t("videoHostOnly"));
    return;
  }
  setRoomVideoQueueBusy(true);
  try {
    const result = await api(`/api/rooms/${encodedCode}/video-queue`, {
      method: "POST",
      body: {
        action,
        ...payload
      }
    });
    if (result?.room) {
      renderRoomInfo(result.room);
    } else {
      syncRoomVideoQueueFromPayload(result);
    }
    if (successToastKey) {
      showToast(t(successToastKey), "success");
    }
    if (refreshChat) {
      await refreshMessages();
    }
  } catch (error) {
    if (error.code === "VIDEO_QUEUE_EMPTY") {
      showToast(t("videoQueueErrorEmpty"));
      return;
    }
    if (error.code === "VIDEO_HISTORY_EMPTY") {
      showToast(t("videoQueueErrorHistoryEmpty"));
      return;
    }
    if (error.code === "VIDEO_QUEUE_ITEM_NOT_FOUND") {
      showToast(t("videoQueueErrorItemNotFound"));
      return;
    }
    showToast(error.message || t("videoSyncFailed"));
  } finally {
    setRoomVideoQueueBusy(false);
  }
}

function playNextRoomVideoFromQueue() {
  return runRoomVideoQueueAction("next", {}, {
    successToastKey: "videoQueueNextDone",
    refreshChat: true
  });
}

function playPreviousRoomVideoFromQueue() {
  return runRoomVideoQueueAction("previous", {}, {
    successToastKey: "videoQueuePrevDone",
    refreshChat: true
  });
}

function removeRoomVideoQueueItem(index) {
  const queueIndex = Number(index);
  if (!Number.isInteger(queueIndex) || queueIndex < 0) {
    return Promise.resolve();
  }
  return runRoomVideoQueueAction("remove", { queueIndex }, {
    successToastKey: "videoQueueRemoveDone",
    refreshChat: false
  });
}

async function transferRoomLeadership(target) {
  const cleanTarget = String(target || "").trim();
  if (!cleanTarget || !isRoomLeader() || cleanTarget === host) {
    return;
  }
  const confirmed = await openActionDialog({
    title: t("transferLeaderTitle"),
    message: fmt(t("transferLeaderConfirm"), { user: displayNameFor(cleanTarget) }),
    confirmText: t("transferLeaderBtn"),
    cancelText: t("closeBtn"),
    confirmClass: "btn btn-approve"
  });
  if (!confirmed) {
    return;
  }
  try {
    await api(`/api/rooms/${encodedCode}/host`, {
      method: "POST",
      body: {
        action: "transfer",
        username: cleanTarget
      }
    });
    showToast(fmt(t("toastLeaderTransferred"), { user: displayNameFor(cleanTarget) }), "success");
    await refreshMessages();
  } catch (error) {
    showToast(error.message);
  }
}

async function claimRoomLeadership() {
  if (!isSupervisor || !me || me === host) {
    return;
  }
  const confirmed = await openActionDialog({
    title: t("claimLeaderTitle"),
    message: t("claimLeaderConfirm"),
    confirmText: t("claimLeaderBtn"),
    cancelText: t("closeBtn"),
    confirmClass: "btn btn-approve"
  });
  if (!confirmed) {
    return;
  }
  try {
    await api(`/api/rooms/${encodedCode}/host`, {
      method: "POST",
      body: { action: "claim" }
    });
    showToast(t("toastLeaderClaimed"), "success");
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

    const actions = document.createElement("div");
    actions.className = "player-actions";

    if (me === host && player !== host) {
      const transferBtn = document.createElement("button");
      transferBtn.className = "player-action-btn transfer-leader-btn";
      transferBtn.type = "button";
      transferBtn.textContent = t("transferLeaderBtn");
      transferBtn.addEventListener("click", async () => {
        sfx("click");
        await transferRoomLeadership(player);
      });
      actions.appendChild(transferBtn);

      const kickBtn = document.createElement("button");
      kickBtn.className = "player-action-btn kick-btn";
      kickBtn.type = "button";
      kickBtn.textContent = t("kickBtn");
      kickBtn.addEventListener("click", async () => {
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
      actions.appendChild(kickBtn);
    }

    if (isSupervisor && me === player && me !== host) {
      const claimBtn = document.createElement("button");
      claimBtn.className = "player-action-btn claim-leader-btn";
      claimBtn.type = "button";
      claimBtn.textContent = t("claimLeaderBtn");
      claimBtn.addEventListener("click", async () => {
        sfx("click");
        await claimRoomLeadership();
      });
      actions.appendChild(claimBtn);
    }

    if (actions.childElementCount > 0) {
      item.appendChild(actions);
    }

    const playerFlagBadge = createCountryFlagBadge(countryCodeForUser(player));
    if (playerFlagBadge) {
      playerFlagBadge.classList.add("player-country-badge");
      item.appendChild(playerFlagBadge);
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
    setMessageReactionsState(messageId, message?.reactions);
  }

  const lineText = document.createElement("div");
  lineText.className = "chat-body-line";
  lineText.textContent = message.text;
  lineWrap.appendChild(lineText);

  const actions = document.createElement("div");
  actions.className = "chat-line-actions";

  const reactBtn = document.createElement("button");
  reactBtn.type = "button";
  reactBtn.className = "chat-line-react-btn";
  reactBtn.textContent = "☺";
  reactBtn.title = t("reactBtn");
  reactBtn.setAttribute("aria-label", t("reactBtn"));
  reactBtn.setAttribute("aria-expanded", "false");
  reactBtn.addEventListener("click", () => {
    sfx("click");
    toggleReactionPicker(lineWrap, messageId);
  });
  actions.appendChild(reactBtn);

  const replyBtn = document.createElement("button");
  replyBtn.type = "button";
  replyBtn.className = "chat-line-reply-btn";
  replyBtn.textContent = t("replyBtn");
  replyBtn.addEventListener("click", () => {
    closeReactionPickers();
    setReplyTargetFromMessage(message);
  });
  actions.appendChild(replyBtn);
  lineWrap.appendChild(actions);

  const picker = document.createElement("div");
  picker.className = "chat-line-reaction-picker hidden";
  CHAT_REACTION_CHOICES.forEach((emoji) => {
    const optionBtn = document.createElement("button");
    optionBtn.type = "button";
    optionBtn.className = "chat-line-reaction-option";
    optionBtn.textContent = emoji;
    optionBtn.title = fmt(t("reactPickOne"), { emoji });
    optionBtn.setAttribute("aria-label", fmt(t("reactPickOne"), { emoji }));
    optionBtn.addEventListener("click", () => {
      sfx("click");
      closeReactionPickers();
      toggleMessageReaction(messageId, emoji);
    });
    picker.appendChild(optionBtn);
  });
  lineWrap.appendChild(picker);

  const reactionRow = document.createElement("div");
  reactionRow.className = "chat-line-reactions hidden";
  lineWrap.appendChild(reactionRow);
  if (messageId) {
    renderMessageReactionChips(lineWrap, messageId);
    updateLineReactionButton(lineWrap, messageId);
  }

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
  if (message?.type === "system" && message?.key === "message_reaction") {
    applyMessageReactionUpdate(message.payload || {});
    return;
  }
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

function stopRoomPolling() {
  if (!pollTimer) {
    return;
  }
  clearInterval(pollTimer);
  pollTimer = null;
}

function restartRoomPolling() {
  stopRoomPolling();
  if (hasLeftRoom || !code || !me) {
    return;
  }
  const intervalMs = roomSocketConnected ? ROOM_POLL_CONNECTED_MS : ROOM_POLL_DISCONNECTED_MS;
  pollTimer = setInterval(() => {
    if (roomVideoPageLeaving || document.visibilityState === "hidden") {
      return;
    }
    refreshMessages().catch(() => {});
  }, intervalMs);
}

function scheduleRoomRefresh(delayMs = ROOM_WS_REFRESH_DEBOUNCE_MS) {
  if (roomRefreshDebounceTimer) {
    return;
  }
  roomRefreshDebounceTimer = setTimeout(() => {
    roomRefreshDebounceTimer = null;
    if (roomVideoPageLeaving || hasLeftRoom) {
      return;
    }
    refreshMessages().catch(() => {});
  }, Math.max(0, Number(delayMs) || 0));
}

function clearRoomSocketReconnectTimer() {
  if (!roomSocketReconnectTimer) {
    return;
  }
  clearTimeout(roomSocketReconnectTimer);
  roomSocketReconnectTimer = null;
}

function closeRoomSocket({ manual = true } = {}) {
  if (manual) {
    roomSocketManualClose = true;
  }
  clearRoomSocketReconnectTimer();
  const activeSocket = roomSocket;
  roomSocket = null;
  roomSocketConnected = false;
  if (!activeSocket) {
    return;
  }
  try {
    activeSocket.close(1000, "client-close");
  } catch (_error) {
    // Ignore close transport errors.
  }
}

function handleRoomSocketPacket(packet) {
  if (!packet || typeof packet !== "object") {
    return;
  }
  const type = String(packet.type || "").trim().toLowerCase();
  if (!type) {
    return;
  }
  if (type === "video_sync") {
    renderRoomVideo({
      video: packet.video || null,
      queue: Array.isArray(packet.queue) ? packet.queue : roomVideoQueueState
    });
    return;
  }
  if (type === "chat_message" || type === "room_changed") {
    scheduleRoomRefresh();
    return;
  }
  if (type === "room_forbidden") {
    redirectToHome(t("toastKickedOut"));
    return;
  }
  if (type === "room_closed") {
    redirectToHome(t("toastRoomClosed"));
  }
}

function connectRoomSocket() {
  if (roomSocket || roomSocketManualClose || !code || !me) {
    return;
  }
  const token = getToken();
  if (!token) {
    return;
  }
  const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${wsProtocol}//${window.location.host}/ws?token=${encodeURIComponent(token)}&room=${encodeURIComponent(code)}`;
  let socket = null;
  try {
    socket = new WebSocket(wsUrl);
  } catch (_error) {
    roomSocketConnected = false;
    restartRoomPolling();
    if (roomSocketManualClose || roomVideoPageLeaving) {
      return;
    }
    roomSocketReconnectAttempt += 1;
    const delay = Math.min(
      ROOM_WS_RECONNECT_MAX_MS,
      ROOM_WS_RECONNECT_BASE_MS * (2 ** Math.min(roomSocketReconnectAttempt, 6))
    );
    clearRoomSocketReconnectTimer();
    roomSocketReconnectTimer = setTimeout(() => {
      roomSocketReconnectTimer = null;
      connectRoomSocket();
    }, delay);
    return;
  }

  roomSocket = socket;
  socket.addEventListener("open", () => {
    if (roomSocket !== socket) {
      return;
    }
    roomSocketConnected = true;
    roomSocketReconnectAttempt = 0;
    clearRoomSocketReconnectTimer();
    restartRoomPolling();
  });

  socket.addEventListener("message", (event) => {
    if (roomSocket !== socket) {
      return;
    }
    let packet = null;
    try {
      packet = JSON.parse(String(event.data || ""));
    } catch (_error) {
      return;
    }
    handleRoomSocketPacket(packet);
  });

  socket.addEventListener("close", () => {
    if (roomSocket !== socket) {
      return;
    }
    roomSocket = null;
    roomSocketConnected = false;
    restartRoomPolling();
    if (roomSocketManualClose || roomVideoPageLeaving || hasLeftRoom) {
      return;
    }
    roomSocketReconnectAttempt += 1;
    const delay = Math.min(
      ROOM_WS_RECONNECT_MAX_MS,
      ROOM_WS_RECONNECT_BASE_MS * (2 ** Math.min(roomSocketReconnectAttempt, 6))
    );
    clearRoomSocketReconnectTimer();
    roomSocketReconnectTimer = setTimeout(() => {
      roomSocketReconnectTimer = null;
      connectRoomSocket();
    }, delay);
  });

  socket.addEventListener("error", () => {
    // Let the close handler handle reconnect behavior.
  });
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
      const list = [];
      if (message.type === "user") {
        list.push(message.user);
        if (message.replyTo?.user) {
          list.push(message.replyTo.user);
        }
        const reactions = normalizeMessageReactionsState(message.reactions);
        CHAT_REACTION_CHOICES.forEach((emoji) => {
          const users = Array.isArray(reactions[emoji]) ? reactions[emoji] : [];
          users.forEach((username) => list.push(username));
        });
      }
      if (message.type === "system" && message.key === "message_reaction") {
        const reactions = normalizeMessageReactionsState(message.payload?.reactions);
        CHAT_REACTION_CHOICES.forEach((emoji) => {
          const users = Array.isArray(reactions[emoji]) ? reactions[emoji] : [];
          users.forEach((username) => list.push(username));
        });
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
  if (quickLeaveBtn) {
    quickLeaveBtn.textContent = t("quickLeave");
    quickLeaveBtn.title = t("quickLeave");
    quickLeaveBtn.setAttribute("aria-label", t("quickLeave"));
  }
  document.getElementById("chatTitle").textContent = t("chatTitle");
  document.getElementById("chatInput").placeholder = t("chatPlaceholder");
  sendBtn.textContent = t("sendBtn");
  if (chatComposerOpenBtn) {
    chatComposerOpenBtn.textContent = "✎";
    chatComposerOpenBtn.title = t("chatComposerOpen");
    chatComposerOpenBtn.setAttribute("aria-label", t("chatComposerOpen"));
  }
  if (chatComposerCloseBtn) {
    chatComposerCloseBtn.textContent = "⌄";
    chatComposerCloseBtn.title = t("chatComposerClose");
    chatComposerCloseBtn.setAttribute("aria-label", t("chatComposerClose"));
  }
  videoTitle.textContent = t("videoTitle");
  videoHintText.textContent = t("videoHint");
  if (videoEmptyNoticeLine1) {
    videoEmptyNoticeLine1.textContent = t("videoEmptyLine1");
  }
  if (videoEmptyNoticeLine2) {
    videoEmptyNoticeLine2.textContent = t("videoEmptyLine2");
  }
  if (videoToolsTitle) {
    videoToolsTitle.textContent = t("videoToolsTitle");
  }
  videoFileLabel.textContent = t("videoFileLabel");
  if (videoYoutubeLabel) {
    videoYoutubeLabel.textContent = t("videoYoutubeLabel");
  }
  if (videoYoutubeInput) {
    videoYoutubeInput.placeholder = t("videoYoutubePlaceholder");
  }
  if (videoQueueTitle) {
    videoQueueTitle.textContent = t("videoQueueTitle");
  }
  if (videoQueuePrevBtn) {
    videoQueuePrevBtn.textContent = t("videoQueuePrevBtn");
  }
  if (videoQueueNextBtn) {
    videoQueueNextBtn.textContent = t("videoQueueNextBtn");
  }
  if (videoQueueEmpty) {
    videoQueueEmpty.textContent = t("videoQueueEmpty");
  }
  if (roomVideoState && roomVideoState.filename) {
    const uploader = videoUploaderName(roomVideoState.uploadedBy);
    if (uploader) {
      videoStatusText.textContent = fmt(t("videoNowPlayingWithUser"), {
        name: roomVideoState.filename,
        user: uploader
      });
    } else {
      videoStatusText.textContent = fmt(t("videoNowPlaying"), { name: roomVideoState.filename });
    }
  } else {
    videoStatusText.textContent = t("videoNoVideo");
  }
  updateVideoEmptyNoticeState();
  setVideoUploadBusy(isUploadingRoomVideo);
  updateRoomVideoQueueUi();
  if (roomVideoAutoNextCountdownSeconds > 0 && roomVideoAutoNextVideoId) {
    updateRoomVideoAutoNextCountdownText(roomVideoAutoNextCountdownSeconds);
  }
  updateRoomVideoUploadOverlayText();
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
  if (openMyProfileBtn) {
    openMyProfileBtn.textContent = t("profileBtn");
  }
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
  postponeActiveModalRequest();
});

if (joinModalTopClose) {
  joinModalTopClose.addEventListener("click", () => {
    sfx("click");
    postponeActiveModalRequest();
  });
}

if (joinRequestModal) {
  joinRequestModal.addEventListener("click", (event) => {
    if (event.target === joinRequestModal) {
      postponeActiveModalRequest();
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

if (openMyProfileBtn) {
  openMyProfileBtn.addEventListener("click", () => {
    sfx("click");
    openProfileModal(me);
  });
}

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
    profileAvatarInput.value = "";
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
  markRoomVideoUserInteracted();
  tryUnlockYouTubeAudioFromGesture();
  const target = event.target;
  const isElementTarget = target instanceof Element;
  const isReactionTarget = isElementTarget
    ? Boolean(target.closest(".chat-line-reaction-picker, .chat-line-react-btn, .chat-line-reaction-chip"))
    : false;
  if (!isReactionTarget && activeReactionPickerMessageId) {
    closeReactionPickers();
  }
  if (target && chatForm && !chatForm.contains(target)) {
    chatLastOutsidePointerAt = Date.now();
  }
  if ((!playersDrawerOpen && !videoToolsOpen) || !isRoomMobileLayout()) {
    return;
  }
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

if (videoYoutubeBtn) {
  videoYoutubeBtn.addEventListener("click", () => {
    sfx("click");
    setRoomYouTubeVideo();
  });
}

if (videoClearBtn) {
  videoClearBtn.addEventListener("click", () => {
    sfx("click");
    clearRoomVideo();
  });
}

if (videoClearTopBtn) {
  videoClearTopBtn.addEventListener("click", () => {
    sfx("click");
    clearRoomVideo();
  });
}

if (videoQueueNextBtn) {
  videoQueueNextBtn.addEventListener("click", () => {
    sfx("click");
    playNextRoomVideoFromQueue();
  });
}

if (videoQueuePrevBtn) {
  videoQueuePrevBtn.addEventListener("click", () => {
    sfx("click");
    playPreviousRoomVideoFromQueue();
  });
}

if (videoYoutubeInput) {
  videoYoutubeInput.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    setRoomYouTubeVideo();
  });
}

if (videoPlayerFrame) {
  const onVideoFrameHover = () => {
    revealRoomVideoControls();
  };
  const onVideoFrameGesture = () => {
    markRoomVideoUserInteracted();
    revealRoomVideoControls();
    tryUnlockYouTubeAudioFromGesture();
  };
  videoPlayerFrame.addEventListener("mousemove", onVideoFrameHover);
  videoPlayerFrame.addEventListener("pointerdown", onVideoFrameGesture);
  videoPlayerFrame.addEventListener("touchstart", onVideoFrameGesture, { passive: true });
}

if (videoPlayPauseBtn) {
  videoPlayPauseBtn.addEventListener("click", () => {
    if (!roomVideoState) {
      return;
    }
    if (!isRoomLeader()) {
      applyRoomVideoSyncToPlayer({ forceSeek: true });
      return;
    }
    sfx("click");
    revealRoomVideoControls();
    if (isYouTubeRoomVideo(roomVideoState)) {
      if (!youTubePlayerReady || !youTubePlayer) {
        if (isRoomLeader()) {
          youTubeLeaderPlayPending = true;
          const youtubeId = parseYouTubeVideoId(roomVideoState?.youtubeId) || parseYouTubeVideoId(roomVideoState?.src);
          if (youtubeId) {
            ensureYouTubePlayer(youtubeId, { autoplay: true }).catch(() => {});
          }
        }
        return;
      }
      const current = getSafeYouTubeCurrentTime(getRoomVideoDuration(), { keepMonotonicWhilePlaying: true });
      if (isYouTubePlaying()) {
        suppressYouTubeLeaderStateBroadcastTemporarily();
        if (typeof youTubePlayer.pauseVideo === "function") {
          youTubePlayer.pauseVideo();
        }
        sendRoomVideoSync("pause", { currentTimeOverride: current });
      } else if (typeof youTubePlayer.playVideo === "function") {
        suppressYouTubeLeaderStateBroadcastTemporarily();
        youTubePlayer.playVideo();
        sendRoomVideoSync("play", { currentTimeOverride: current });
      }
      return;
    }
    if (!roomVideoPlayer) {
      return;
    }
    if (roomVideoPlayer.paused || roomVideoPlayer.ended) {
      const current = getSafeLocalVideoCurrentTime(getRoomVideoDuration());
      roomVideoPlayer.play().catch(() => {});
      sendRoomVideoSync("play", { currentTimeOverride: current });
      return;
    }
    const current = getSafeLocalVideoCurrentTime(getRoomVideoDuration());
    roomVideoPlayer.pause();
    sendRoomVideoSync("pause", { currentTimeOverride: current });
  });
}

if (videoVolumeBtn) {
  videoVolumeBtn.addEventListener("click", () => {
    if (!roomVideoState) {
      return;
    }
    sfx("click");
    markRoomVideoUserInteracted();
    revealRoomVideoControls();
    const effectiveVolume = getEffectiveRoomVideoVolume();
    if (effectiveVolume <= 0.001 || roomVideoLocalMuted) {
      const restoreVolume = roomVideoVolumeBeforeMute > 0.001
        ? roomVideoVolumeBeforeMute
        : ROOM_VIDEO_DEFAULT_VOLUME;
      setRoomVideoVolume(restoreVolume, { mute: false, persist: true });
      tryUnlockYouTubeAudioFromGesture();
      return;
    }
    roomVideoVolumeBeforeMute = effectiveVolume;
    setRoomVideoVolume(effectiveVolume, { mute: true, persist: true });
  });
}

if (videoVolumeRange) {
  videoVolumeRange.addEventListener("input", () => {
    if (!roomVideoState) {
      return;
    }
    markRoomVideoUserInteracted();
    revealRoomVideoControls();
    const nextVolume = clampRoomVideoVolume(Number(videoVolumeRange.value || 0) / 100);
    setRoomVideoVolume(nextVolume, { mute: nextVolume <= 0.001, persist: true });
    if (nextVolume > 0.001) {
      tryUnlockYouTubeAudioFromGesture();
    }
  });
}

if (videoSeekRange) {
  videoSeekRange.max = String(ROOM_VIDEO_SEEK_RANGE_MAX);
  videoSeekRange.step = "1";

  videoSeekRange.addEventListener("input", () => {
    if (!isRoomLeader()) {
      roomVideoSeekDragging = false;
      updateRoomVideoControls();
      return;
    }
    roomVideoSeekDragging = true;
    revealRoomVideoControls();
    const duration = getRoomVideoDuration();
    const progress = Number(videoSeekRange.value || 0) / ROOM_VIDEO_SEEK_RANGE_MAX;
    const previewTime = duration > 0 ? progress * duration : 0;
    updateRoomVideoControls({ previewTime });
  });

  videoSeekRange.addEventListener("change", () => {
    if (!isRoomLeader()) {
      roomVideoSeekDragging = false;
      applyRoomVideoSyncToPlayer({ forceSeek: true });
      updateRoomVideoControls();
      return;
    }
    if (!roomVideoState) {
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
    const progress = Number(videoSeekRange.value || 0) / ROOM_VIDEO_SEEK_RANGE_MAX;
    const targetTime = clampVideoTime(progress * duration, duration);
    roomVideoLastKnownTime = targetTime;
    if (isYouTubeRoomVideo(roomVideoState)) {
      if (!isRoomLeader()) {
        roomVideoSeekDragging = false;
        showVideoControlLockedToast();
        applyRoomVideoSyncToPlayer({ forceSeek: true });
        return;
      }
      if (youTubePlayerReady && youTubePlayer && typeof youTubePlayer.seekTo === "function") {
        allowRoomVideoRewindTemporarily();
        youTubePlayer.seekTo(targetTime, true);
      }
      sendRoomVideoSync("seek", { currentTimeOverride: targetTime, allowRewind: true });
    } else if (roomVideoPlayer) {
      roomVideoPlayer.currentTime = targetTime;
      sendRoomVideoSync("seek", { currentTimeOverride: targetTime, allowRewind: true });
    }
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

  roomVideoPlayer.addEventListener("loadedmetadata", async () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    roomVideoMetadataReady = true;
    revealRoomVideoControls();
    updateRoomVideoControls();
    await applyRoomVideoSyncToPlayer({ forceSeek: true });
  });

  roomVideoPlayer.addEventListener("play", () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    scheduleRoomVideoControlsAutoHide();
    updateRoomVideoControls();
    handleRoomVideoControlEvent("play");
  });

  roomVideoPlayer.addEventListener("pause", () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    revealRoomVideoControls();
    updateRoomVideoControls();
    if (roomVideoPlayer.ended) {
      return;
    }
    handleRoomVideoControlEvent("pause");
  });

  roomVideoPlayer.addEventListener("seeked", () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    updateRoomVideoControls();
    handleRoomVideoControlEvent("seek");
  });

  roomVideoPlayer.addEventListener("ratechange", () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    updateRoomVideoControls();
    handleRoomVideoControlEvent("rate");
  });

  roomVideoPlayer.addEventListener("timeupdate", () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    const duration = getRoomVideoDuration();
    const current = Number(roomVideoPlayer.currentTime);
    if (Number.isFinite(current) && current >= 0) {
      roomVideoLastKnownTime = clampVideoTime(current, duration);
    }
    if (!roomVideoSeekDragging) {
      updateRoomVideoControls();
    }
  });

  roomVideoPlayer.addEventListener("ended", () => {
    if (isYouTubeRoomVideo(roomVideoState)) {
      return;
    }
    revealRoomVideoControls();
    updateRoomVideoControls();
    if (!roomVideoState) {
      handleRoomVideoControlEvent("stop");
      return;
    }
    const duration = getRoomVideoDuration();
    let current = getSafeLocalVideoCurrentTime(duration);
    const known = clampVideoTime(roomVideoLastKnownTime, duration);
    if (current <= 0.05 && known > 0.25) {
      current = known;
    }
    // Some browsers can emit a transient "ended" after seek even when not at video end.
    if (duration > 2 && current < duration - 1.4) {
      applyRoomVideoSyncToPlayer({ forceSeek: true });
      return;
    }
    const endTime = duration > 0 && current >= duration - 0.35 ? duration : current;
    roomVideoLastKnownTime = endTime;
    const endedVideoId = String(activeRoomVideoId || roomVideoState?.id || "").trim();
    if (roomVideoQueueState.length > 0) {
      startRoomVideoAutoNextCountdown(endedVideoId);
      if (isRoomLeader()) {
        handleLeaderVideoEndedAutoAdvance(endTime);
      }
      return;
    }
    cancelRoomVideoAutoNextCountdown();
    if (isRoomLeader()) {
      handleLeaderVideoEndedAutoAdvance(endTime);
      return;
    }
    applyRoomVideoSyncToPlayer({ forceSeek: true });
  });

  roomVideoPlayer.addEventListener("contextmenu", (event) => {
    event.preventDefault();
  });
}

document.addEventListener("keydown", (event) => {
  markRoomVideoUserInteracted();
  tryUnlockYouTubeAudioFromGesture();
  if (event.key === "Escape" && kickSupervisorDeniedModal && !kickSupervisorDeniedModal.classList.contains("hidden")) {
    closeKickSupervisorDeniedModal();
    return;
  }
  if (event.key === "Escape" && selfKickJokeModal && !selfKickJokeModal.classList.contains("hidden")) {
    closeSelfKickJokeModal();
    return;
  }
  if (event.key === "Escape" && joinRequestModal && !joinRequestModal.classList.contains("hidden")) {
    postponeActiveModalRequest();
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
  if (event.key === "Escape" && activeReactionPickerMessageId) {
    closeReactionPickers();
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

const markInteractionForRoomVideoAudio = () => {
  markRoomVideoUserInteracted();
  attemptMobileRoomVideoResume({ fromGesture: true });
  tryUnlockYouTubeAudioFromGesture();
};

document.addEventListener("pointerdown", markInteractionForRoomVideoAudio, { passive: true });
document.addEventListener("touchstart", markInteractionForRoomVideoAudio, { passive: true });

if (roomMobileLayoutQuery && typeof roomMobileLayoutQuery.addEventListener === "function") {
  roomMobileLayoutQuery.addEventListener("change", () => {
    syncPlayersDrawerMode();
    syncMobileChatKeyboardOffset({ keepLatestVisible: true });
  });
}

function leaveRoomToLobby() {
  roomVideoPageLeaving = true;
  sfx("leave");
  leaveCurrentRoom({ keepalive: true }).catch(() => {});
  window.location.href = "/";
}

backToLobbyLink.addEventListener("click", async (event) => {
  event.preventDefault();
  leaveRoomToLobby();
});

if (quickLeaveBtn) {
  quickLeaveBtn.addEventListener("click", (event) => {
    event.preventDefault();
    leaveRoomToLobby();
  });
}

if (chatReplyCancelBtn) {
  chatReplyCancelBtn.addEventListener("click", () => {
    sfx("click");
    clearReplyTarget();
  });
}

if (chatComposerOpenBtn) {
  chatComposerOpenBtn.addEventListener("click", () => {
    sfx("click");
    setMobileChatComposerOpen(true, { focusInput: true });
  });
}

if (chatComposerCloseBtn) {
  chatComposerCloseBtn.addEventListener("click", () => {
    sfx("click");
    closeMobileChatComposerFromUserAction();
  });
}

if (sendBtn) {
  sendBtn.addEventListener(
    "pointerdown",
    (event) => {
      if (!isRoomMobileLayout()) {
        return;
      }
      // Keep focus on the input so mobile keyboard does not collapse on send tap.
      if (event.pointerType && event.pointerType !== "mouse") {
        event.preventDefault();
        suppressNextMobileSendButtonClickUntil = Date.now() + 700;
        focusChatInputForContinuousTyping(0, { force: true });
        if (!isSendingChatMessage) {
          if (typeof chatForm.requestSubmit === "function") {
            chatForm.requestSubmit();
          } else {
            chatForm.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
          }
        }
      }
    },
    { passive: false }
  );
  sendBtn.addEventListener("click", (event) => {
    if (!isRoomMobileLayout()) {
      return;
    }
    if (Date.now() <= suppressNextMobileSendButtonClickUntil) {
      event.preventDefault();
      event.stopPropagation();
      return;
    }
    focusChatInputForContinuousTyping(0, { force: true });
  });
}

if (chatInput) {
  chatInput.addEventListener("focus", () => {
    if (!isRoomMobileLayout()) {
      return;
    }
    setMobileChatComposerOpen(true);
    [0, 80, 170, 300].forEach((delayMs) => {
      setTimeout(() => {
        syncMobileChatKeyboardOffset({ keepLatestVisible: true });
      }, delayMs);
    });
  });
  chatInput.addEventListener("input", () => {
    if (!isRoomMobileLayout()) {
      return;
    }
    syncMobileChatKeyboardOffset({ keepLatestVisible: true });
  });
  chatInput.addEventListener("blur", () => {
    if (!isRoomMobileLayout()) {
      return;
    }
    [0, 90, 180].forEach((delayMs) => {
      setTimeout(() => {
        syncMobileChatKeyboardOffset({ keepLatestVisible: true });
      }, delayMs);
    });
  });
}

if (chatMessages) {
  const closeComposerFromChatSurface = () => {
    closeMobileChatComposerFromUserAction();
  };
  chatMessages.addEventListener("pointerdown", closeComposerFromChatSurface);
  chatMessages.addEventListener("touchstart", closeComposerFromChatSurface, { passive: true });
}

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isSendingChatMessage) {
    return;
  }
  const submitStartedAt = Date.now();
  const text = String(chatInput.value || "").trim();
  if (!text) {
    return;
  }
  if (isRoomMobileLayout()) {
    setMobileChatComposerOpen(true);
  }
  const replyToMessageId = Number(activeReplyTarget?.id || 0);
  const clientMessageId = reserveClientMessageId(text, replyToMessageId);
  setChatSendingState(true);
  focusChatInputForContinuousTyping(submitStartedAt, { force: isRoomMobileLayout() });

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
    focusChatInputForContinuousTyping(submitStartedAt, { force: isRoomMobileLayout() });
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
    roomSocketManualClose = false;
    connectRoomSocket();
    restartRoomPolling();
    ensureRoomVideoSyncHeartbeat();
  } catch (error) {
    redirectToHome(error.message);
  }
}

window.addEventListener("beforeunload", () => {
  roomVideoPageLeaving = true;
  document.body.classList.remove("room-page");
  document.body.classList.remove("room-mobile-keyboard-open");
  document.documentElement.classList.remove("room-page");
  document.documentElement.style.setProperty("--room-chat-keyboard-offset", "0px");
  closeRoomSocket({ manual: true });
  stopRoomPolling();
  if (roomRefreshDebounceTimer) {
    clearTimeout(roomRefreshDebounceTimer);
    roomRefreshDebounceTimer = null;
  }
  leaveCurrentRoom({ keepalive: true });
  setRoomSupervisorOpen(false);
  hideGlobalAnnouncement();
  queuedAnnouncement = null;
  closeRequestModal();
  hideFullscreenChatNotice();
  hideRoomVideoUploadOverlay();
  clearRoomVideoPlayer();
  unlockOrientationAfterFullscreen();
});

window.addEventListener("pagehide", () => {
  roomVideoPageLeaving = true;
  if (roomVideoSyncState?.playing) {
    roomVideoResumePending = true;
  }
});

window.addEventListener("pageshow", () => {
  roomVideoPageLeaving = false;
  if (me && !hasLeftRoom) {
    if (!roomSocket && !roomSocketManualClose) {
      connectRoomSocket();
    }
    restartRoomPolling();
  }
  if (roomVideoSyncState?.playing) {
    roomVideoResumePending = true;
  }
  if (roomVideoState && roomVideoSyncState) {
    applyRoomVideoSyncToPlayer({ forceSeek: true });
  }
  setTimeout(() => {
    attemptMobileRoomVideoResume({ fromGesture: false });
  }, 90);
  [40, 130, 240].forEach((delayMs) => {
    setTimeout(() => {
      syncMobileChatKeyboardOffset({ keepLatestVisible: true });
    }, delayMs);
  });
});

document.addEventListener("visibilitychange", () => {
  const isVisible = document.visibilityState === "visible";
  roomVideoPageLeaving = !isVisible;
  if (!isVisible) {
    stopRoomPolling();
    if (roomVideoSyncState?.playing) {
      roomVideoResumePending = true;
    }
    return;
  }
  if (me && !hasLeftRoom) {
    if (!roomSocket && !roomSocketManualClose) {
      connectRoomSocket();
    }
    restartRoomPolling();
  }
  if (roomVideoSyncState?.playing) {
    roomVideoResumePending = true;
  }
  if (roomVideoState && roomVideoSyncState) {
    if (!isYouTubeRoomVideo(roomVideoState) && roomVideoPlayer && roomVideoPlayer.readyState < 1) {
      try {
        roomVideoPlayer.load();
      } catch (_error) {
        // Ignore reload failures and apply sync using available state.
      }
    }
    applyRoomVideoSyncToPlayer({ forceSeek: true });
  }
  if (roomVideoPlayer && roomVideoUserInteracted) {
    applyLocalRoomVideoVolumeToPlayers();
  }
  syncMobileChatKeyboardOffset({ keepLatestVisible: true });
  attemptMobileRoomVideoResume({ fromGesture: false });
  youTubeAudioUnlockNeeded = true;
  setTimeout(() => {
    attemptMobileRoomVideoResume({ fromGesture: false });
    tryUnlockYouTubeAudioFromGesture();
  }, 120);
  refreshMessages().catch(() => {});
});

loadRoomVideoVolumePreference();
setRoomVideoVolume(roomVideoLocalVolume, { mute: roomVideoLocalMuted, persist: false });
updateRoomVideoControls();

bindMobileChatViewportEvents();
setLang(getLang());
syncPlayersDrawerMode();
syncMobileChatKeyboardOffset({ keepLatestVisible: false });
bootRoom();



