const LANG_KEY = "bedna_lang";
const TOKEN_KEY = "bedna_token";
const USER_KEY = "bedna_user";

const registerForm = document.getElementById("registerForm");
const registerUsernameInput = document.getElementById("registerUsername");
const registerUsernameError = document.getElementById("registerUsernameError");
const registerPasswordInput = document.getElementById("registerPassword");
const registerDisplayNameInput = document.getElementById("registerDisplayName");
const registerAvatarInput = document.getElementById("registerAvatarInput");
const registerSubmitBtn = document.getElementById("registerSubmitBtn");
const registerRemoveAvatarBtn = document.getElementById("registerRemoveAvatarBtn");
const backToLoginBtn = document.getElementById("backToLoginBtn");
const langSelect = document.getElementById("langSelect");
const toastContainer = document.getElementById("registerToastContainer");

let selectedAvatarDataUrl = null;
const REGISTERED_USERNAME_REGEX = /^[A-Za-z0-9]+$/;
const AVATAR_MAX_DATA_URL_LENGTH = 280000;
const AVATAR_DIMENSION_STEPS = [720, 600, 512, 420, 360, 300, 256];
const AVATAR_QUALITY_STEPS = [0.86, 0.78, 0.7, 0.62, 0.54, 0.46, 0.38];
let avatarWebpSupported = null;

const I18N = {
  ar: {
    pageTitle: "إنشاء حساب - SawaWatch",
    langLabel: "اللغة",
    registerTitle: "إنشاء حساب",
    registerDesc: "أكمل البيانات لإنشاء حساب جديد.",
    registerUsernameLabel: "اسم المستخدم",
    registerPasswordLabel: "كلمة المرور",
    registerDisplayNameLabel: "اسم الحساب",
    registerAvatarLabel: "الصورة الشخصية (اختياري)",
    registerSubmitBtn: "إنشاء حساب",
    registerRemoveAvatarBtn: "إزالة الصورة",
    usernameInlineInvalid: "اسم المستخدم يجب أن يكون 3-30 حرفًا إنجليزيًا/رقميًا فقط.",
    backToLoginBtn: "العودة لتسجيل الدخول",
    toastInputInvalid: "اسم المستخدم يجب أن يكون 3-30 حرفًا إنجليزيًا/رقميًا فقط، وكلمة المرور 4+، واسم الحساب 2+ أحرف.",
    toastRegisterOk: "تم إنشاء الحساب بنجاح.",
    toastFailed: "فشل الطلب.",
    toastNetworkError: "تعذر الاتصال بالخادم. تأكد من الإنترنت وحاول مرة أخرى.",
    toastAvatarReadFailed: "تعذر قراءة الصورة.",
    toastAvatarTooLarge: "الصورة كبيرة جدًا. اختر صورة أصغر أو أقل دقة.",
    toastProfileSaveFailed: "تم إنشاء الحساب لكن تعذر حفظ بيانات الملف الشخصي."
  },
  en: {
    pageTitle: "Create Account - SawaWatch",
    langLabel: "Language",
    registerTitle: "Create Account",
    registerDesc: "Complete the fields to create a new account.",
    registerUsernameLabel: "Username",
    registerPasswordLabel: "Password",
    registerDisplayNameLabel: "Account Name",
    registerAvatarLabel: "Profile Image (Optional)",
    registerSubmitBtn: "Create Account",
    registerRemoveAvatarBtn: "Remove Image",
    usernameInlineInvalid: "Username must be 3-30 English letters/numbers only.",
    backToLoginBtn: "Back to Login",
    toastInputInvalid: "Username must be 3-30 English letters/numbers only, password 4+, and account name 2+ chars.",
    toastRegisterOk: "Account created successfully.",
    toastFailed: "Request failed.",
    toastNetworkError: "Could not reach server. Check your connection and try again.",
    toastAvatarReadFailed: "Failed to read image.",
    toastAvatarTooLarge: "Image is too large. Choose a smaller one.",
    toastProfileSaveFailed: "Account created, but profile data could not be saved."
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
  return I18N[lang][key] || I18N.ar[key] || key;
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

function isValidRegisteredUsername(username) {
  const value = String(username || "").trim();
  return value.length >= 3 && value.length <= 30 && REGISTERED_USERNAME_REGEX.test(value);
}

function setUsernameInlineError(visible) {
  if (!registerUsernameError || !registerUsernameInput) {
    return;
  }
  if (visible) {
    registerUsernameError.textContent = t("usernameInlineInvalid");
    registerUsernameError.classList.remove("hidden");
    registerUsernameInput.setAttribute("aria-invalid", "true");
    return;
  }
  registerUsernameError.textContent = "";
  registerUsernameError.classList.add("hidden");
  registerUsernameInput.removeAttribute("aria-invalid");
}

function showToast(message) {
  if (!toastContainer) {
    return;
  }
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  toastContainer.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add("show"));
  setTimeout(() => {
    toast.classList.remove("show");
    setTimeout(() => toast.remove(), 200);
  }, 2400);
}

async function api(pathname, options = {}) {
  let response;
  try {
    response = await fetch(pathname, {
      method: options.method || "GET",
      headers: {
        "Content-Type": "application/json",
        "X-Lang": getLang(),
        ...(CLIENT_COUNTRY_CODE ? { "X-Country": CLIENT_COUNTRY_CODE } : {}),
        ...(options.token ? { Authorization: `Bearer ${options.token}` } : {})
      },
      body: options.body ? JSON.stringify(options.body) : undefined
    });
  } catch (_error) {
    const error = new Error(t("toastNetworkError"));
    error.code = "NETWORK_ERROR";
    throw error;
  }

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data.error || t("toastFailed"));
    error.status = response.status;
    error.code = data.code || "";
    error.data = data;
    throw error;
  }
  return data;
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
      reject(new Error(t("toastAvatarReadFailed")));
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
    throw new Error(t("toastAvatarReadFailed"));
  }
  const image = await loadImageFromFile(file);
  const sourceWidth = Math.max(1, Number(image.naturalWidth || image.width || 0));
  const sourceHeight = Math.max(1, Number(image.naturalHeight || image.height || 0));
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d", { alpha: false });
  if (!ctx) {
    throw new Error(t("toastAvatarReadFailed"));
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
  throw new Error(t("toastAvatarTooLarge"));
}

function applyTranslations() {
  document.title = t("pageTitle");
  document.getElementById("langLabel").textContent = t("langLabel");
  document.getElementById("registerTitle").textContent = t("registerTitle");
  document.getElementById("registerDesc").textContent = t("registerDesc");
  document.getElementById("registerUsernameLabel").textContent = t("registerUsernameLabel");
  document.getElementById("registerPasswordLabel").textContent = t("registerPasswordLabel");
  document.getElementById("registerDisplayNameLabel").textContent = t("registerDisplayNameLabel");
  document.getElementById("registerAvatarLabel").textContent = t("registerAvatarLabel");
  registerSubmitBtn.textContent = t("registerSubmitBtn");
  registerRemoveAvatarBtn.textContent = t("registerRemoveAvatarBtn");
  backToLoginBtn.textContent = t("backToLoginBtn");
  if (registerUsernameError && !registerUsernameError.classList.contains("hidden")) {
    registerUsernameError.textContent = t("usernameInlineInvalid");
  }
}

registerUsernameInput.addEventListener("input", () => {
  if (!registerUsernameError || registerUsernameError.classList.contains("hidden")) {
    return;
  }
  setUsernameInlineError(!isValidRegisteredUsername(registerUsernameInput.value));
});

registerAvatarInput.addEventListener("change", async (event) => {
  const file = event.target.files?.[0];
  if (!file) {
    return;
  }
  try {
    selectedAvatarDataUrl = await readFileAsDataUrl(file);
  } catch (error) {
    registerAvatarInput.value = "";
    showToast(error.message || t("toastAvatarReadFailed"));
  }
});

registerRemoveAvatarBtn.addEventListener("click", () => {
  selectedAvatarDataUrl = "";
  registerAvatarInput.value = "";
});

registerForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const username = String(registerUsernameInput.value || "").trim();
  const password = String(registerPasswordInput.value || "");
  const displayName = String(registerDisplayNameInput.value || "").trim();

  if (!isValidRegisteredUsername(username)) {
    setUsernameInlineError(true);
    registerUsernameInput.focus();
    return;
  }
  setUsernameInlineError(false);

  if (password.length < 4 || displayName.length < 2) {
    showToast(t("toastInputInvalid"));
    return;
  }

  registerSubmitBtn.disabled = true;
  try {
    const registerResult = await api("/api/register", {
      method: "POST",
      body: { username, password }
    });
    localStorage.setItem(TOKEN_KEY, registerResult.token);
    localStorage.setItem(USER_KEY, registerResult.username);

    const profilePayload = { displayName };
    if (selectedAvatarDataUrl !== null) {
      profilePayload.avatarDataUrl = selectedAvatarDataUrl;
    }
    try {
      await api("/api/profile", {
        method: "PATCH",
        token: registerResult.token,
        body: profilePayload
      });
    } catch (profileError) {
      showToast(profileError.message || t("toastProfileSaveFailed"));
    }

    window.location.href = `/?msg=${encodeURIComponent(t("toastRegisterOk"))}`;
  } catch (error) {
    showToast(error.message || t("toastFailed"));
  } finally {
    registerSubmitBtn.disabled = false;
  }
});

backToLoginBtn.addEventListener("click", () => {
  window.location.href = "/";
});

langSelect.addEventListener("change", (event) => {
  setLang(event.target.value);
});

setLang(getLang());
