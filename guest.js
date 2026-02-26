const LANG_KEY = "bedna_lang";
const TOKEN_KEY = "bedna_token";
const USER_KEY = "bedna_user";

const guestForm = document.getElementById("guestForm");
const guestNameInput = document.getElementById("guestName");
const guestSubmitBtn = document.getElementById("guestSubmitBtn");
const backToLoginBtn = document.getElementById("backToLoginBtn");
const langSelect = document.getElementById("langSelect");
const toastContainer = document.getElementById("guestToastContainer");
const AUTH_MANDATORY_NOTICE_MS = 5000;
let authMandatoryOverlay = null;
let authMandatoryTitle = null;
let authMandatoryText = null;

const I18N = {
  ar: {
    pageTitle: "الدخول كضيف - SawaWatch",
    langLabel: "اللغة",
    guestTitle: "الدخول كضيف",
    guestDesc: "اكتب اسمك فقط وسيتم إدخالك للموقع مباشرة بدون حساب.",
    guestNameLabel: "الاسم",
    guestSubmitBtn: "دخول كضيف",
    backToLoginBtn: "العودة لتسجيل الدخول",
    toastNameInvalid: "الاسم يجب أن يكون بين 2 و30 حرفًا.",
    toastGuestEnter: "تم الدخول كضيف.",
    authMandatoryTitle: "تنبيه",
    authMandatoryLine1: "مرحبًا بك في سوا واتش، الموقع نسخة تجريبية ومن المحتمل مواجهة مشاكل.",
    authMandatoryLine2: "شكرًا على تفهمكم.",
    toastFailed: "فشل الطلب.",
    toastNetworkError: "تعذر الاتصال بالخادم. تأكد من الإنترنت وحاول مرة أخرى."
  },
  en: {
    pageTitle: "Guest Login - SawaWatch",
    langLabel: "Language",
    guestTitle: "Continue as Guest",
    guestDesc: "Enter your name only, and you will join the site without creating an account.",
    guestNameLabel: "Name",
    guestSubmitBtn: "Enter as Guest",
    backToLoginBtn: "Back to Login",
    toastNameInvalid: "Name must be 2-30 characters.",
    toastGuestEnter: "Entered as guest.",
    authMandatoryTitle: "Notice",
    authMandatoryLine1: "Welcome to SawaWatch. This website is an experimental version and you may encounter issues.",
    authMandatoryLine2: "Thank you for your understanding.",
    toastFailed: "Request failed.",
    toastNetworkError: "Could not reach server. Check your connection and try again."
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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureAuthMandatoryOverlay() {
  if (authMandatoryOverlay) {
    return authMandatoryOverlay;
  }
  authMandatoryOverlay = document.createElement("div");
  authMandatoryOverlay.className = "modal-overlay forced-announcement-overlay auth-mandatory-overlay hidden";
  authMandatoryOverlay.innerHTML = `
    <div class="modal-card forced-announcement-card auth-mandatory-card" role="dialog" aria-modal="true">
      <h3 id="guestAuthMandatoryTitle"></h3>
      <p id="guestAuthMandatoryText" class="forced-announcement-text auth-mandatory-text"></p>
    </div>
  `;
  authMandatoryOverlay.addEventListener("click", (event) => {
    if (event.target === authMandatoryOverlay) {
      event.preventDefault();
    }
  });
  document.body.appendChild(authMandatoryOverlay);
  authMandatoryTitle = authMandatoryOverlay.querySelector("#guestAuthMandatoryTitle");
  authMandatoryText = authMandatoryOverlay.querySelector("#guestAuthMandatoryText");
  return authMandatoryOverlay;
}

async function showAuthMandatoryNotice() {
  const overlay = ensureAuthMandatoryOverlay();
  if (authMandatoryTitle) {
    authMandatoryTitle.textContent = t("authMandatoryTitle");
  }
  if (authMandatoryText) {
    authMandatoryText.textContent = `${t("authMandatoryLine1")}\n${t("authMandatoryLine2")}`;
  }
  overlay.classList.remove("hidden");
  document.body.classList.add("auth-mandatory-lock");
  await delay(AUTH_MANDATORY_NOTICE_MS);
  overlay.classList.add("hidden");
  document.body.classList.remove("auth-mandatory-lock");
}

async function api(pathname, options = {}) {
  let response;
  try {
    response = await fetch(pathname, {
      method: options.method || "GET",
      headers: {
        "Content-Type": "application/json",
        "X-Lang": getLang(),
        ...(CLIENT_COUNTRY_CODE ? { "X-Country": CLIENT_COUNTRY_CODE } : {})
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
    throw error;
  }
  return data;
}

function applyTranslations() {
  document.title = t("pageTitle");
  document.getElementById("langLabel").textContent = t("langLabel");
  document.getElementById("guestTitle").textContent = t("guestTitle");
  document.getElementById("guestDesc").textContent = t("guestDesc");
  document.getElementById("guestNameLabel").textContent = t("guestNameLabel");
  guestSubmitBtn.textContent = t("guestSubmitBtn");
  backToLoginBtn.textContent = t("backToLoginBtn");
}

guestForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const name = String(guestNameInput.value || "").trim();
  if (name.length < 2 || name.length > 30) {
    showToast(t("toastNameInvalid"));
    return;
  }

  guestSubmitBtn.disabled = true;
  try {
    const result = await api("/api/guest-login", {
      method: "POST",
      body: { name }
    });
    localStorage.setItem(TOKEN_KEY, result.token);
    localStorage.setItem(USER_KEY, result.username);
    await showAuthMandatoryNotice();
    window.location.href = `/?msg=${encodeURIComponent(t("toastGuestEnter"))}`;
  } catch (error) {
    showToast(error.message || t("toastFailed"));
  } finally {
    guestSubmitBtn.disabled = false;
  }
});

backToLoginBtn.addEventListener("click", () => {
  window.location.href = "/";
});

langSelect.addEventListener("change", (event) => {
  setLang(event.target.value);
});

setLang(getLang());
