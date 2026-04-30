// Same client-side Google OAuth gate used by the -3W Booking Dashboard.
// This protects the UI and shares the existing dashboard session keys.
(function () {
  const GOOGLE_CLIENT_ID = "409330651463-giie223egsskdq10etn642gjtron1hq5.apps.googleusercontent.com";
  const ALLOWED_DOMAINS = ["ekmtc.com"];
  const SCOPES = "email profile openid";
  const DASHBOARD_ROOT = "/kmtc-3w-dashboard-web/";
  const RETURN_PATH_KEY = "obtReturnPath";
  const LOCAL_AUTH_BYPASS = ["localhost", "127.0.0.1"].includes(location.hostname);

  let currentUser = null;
  let readyCallback = null;

  function language() {
    return localStorage.getItem("obtExceptionLang") === "en" ? "en" : "ko";
  }

  function text(ko, en) {
    return language() === "en" ? en : ko;
  }

  function loginHtml() {
    return `
      <div class="login-box">
        <h2>OBT Exception Monitor</h2>
        <p>${text("회사 Google 계정으로 로그인하세요", "Sign in with company Google account")}</p>
        <button class="google-login" id="loginBtn" type="button">
          <svg width="18" height="18" viewBox="0 0 48 48" aria-hidden="true"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/><path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/></svg>
          ${text("Google 계정으로 로그인", "Sign in with Google")}
        </button>
        <p class="error" id="loginErr"></p>
      </div>
    `;
  }

  function renderLogin() {
    const login = document.getElementById("login");
    if (!login) return;
    login.innerHTML = loginHtml();
    const button = document.getElementById("loginBtn");
    if (button) button.addEventListener("click", doLogin);
  }

  function redirectUri() {
    if (location.hostname.endsWith("github.io")) return location.origin + DASHBOARD_ROOT;
    return location.origin + location.pathname;
  }

  function doLogin() {
    sessionStorage.setItem(RETURN_PATH_KEY, location.pathname + location.search);
    const authUrl = "https://accounts.google.com/o/oauth2/v2/auth"
      + "?client_id=" + encodeURIComponent(GOOGLE_CLIENT_ID)
      + "&redirect_uri=" + encodeURIComponent(redirectUri())
      + "&response_type=token"
      + "&scope=" + encodeURIComponent(SCOPES)
      + "&include_granted_scopes=true"
      + "&prompt=select_account";
    location.href = authUrl;
  }

  function setError(message) {
    const login = document.getElementById("login");
    if (login && !login.innerHTML.trim()) renderLogin();
    const error = document.getElementById("loginErr");
    if (!error) return;
    error.style.display = "block";
    error.textContent = message;
  }

  async function userInfo(token) {
    const response = await fetch("https://www.googleapis.com/oauth2/v2/userinfo", {
      headers: { Authorization: "Bearer " + token }
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  }

  function domainAllowed(email) {
    const domain = String(email || "").split("@")[1] || "";
    return !ALLOWED_DOMAINS.length || ALLOWED_DOMAINS.includes(domain.toLowerCase());
  }

  function saveSession(token, info) {
    currentUser = { email: info.email, name: info.name || info.email, picture: info.picture };
    sessionStorage.setItem("gtoken", token);
    sessionStorage.setItem("guser", JSON.stringify(currentUser));
  }

  async function handleRedirect() {
    const hash = location.hash.substring(1);
    if (!hash) return false;
    const params = new URLSearchParams(hash);
    const token = params.get("access_token");
    if (!token) return false;
    history.replaceState(null, "", location.pathname + location.search);

    try {
      const info = await userInfo(token);
      if (!domainAllowed(info.email)) {
        const domain = String(info.email || "").split("@")[1] || "";
        setError(`${domain} ${text("도메인은 접근할 수 없습니다.", "domain is not allowed.")}`);
        return true;
      }
      saveSession(token, info);
      showApp();
    } catch (error) {
      setError(text("사용자 정보 확인 실패: ", "Failed to verify user: ") + error.message);
    }
    return true;
  }

  async function validateStoredSession() {
    const token = sessionStorage.getItem("gtoken");
    const userText = sessionStorage.getItem("guser");
    if (!token || !userText) return false;
    try {
      const response = await fetch("https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=" + token);
      if (!response.ok) throw new Error("expired");
      currentUser = JSON.parse(userText);
      if (!domainAllowed(currentUser.email)) throw new Error("domain");
      showApp();
      return true;
    } catch (error) {
      sessionStorage.removeItem("gtoken");
      sessionStorage.removeItem("guser");
      return false;
    }
  }

  function showApp() {
    const login = document.getElementById("login");
    const app = document.getElementById("app");
    const userInfoEl = document.getElementById("userInfo");
    if (login) login.style.display = "none";
    if (app) app.style.display = "block";
    refreshUser();
    if (readyCallback) readyCallback(currentUser);
  }

  function refreshUser() {
    const userInfoEl = document.getElementById("userInfo");
    if (userInfoEl && currentUser) {
      userInfoEl.textContent = `${currentUser.name || currentUser.email} | ${text("로그아웃", "Logout")}`;
      userInfoEl.title = text("로그아웃", "Logout");
    }
  }

  function logout() {
    sessionStorage.removeItem("gtoken");
    sessionStorage.removeItem("guser");
    sessionStorage.removeItem(RETURN_PATH_KEY);
    location.reload();
  }

  async function requireAuth(options = {}) {
    readyCallback = options.onReady || null;
    renderLogin();
    if (LOCAL_AUTH_BYPASS) {
      currentUser = { email: "local@ekmtc.com", name: "Local Preview" };
      showApp();
      return;
    }
    if (await handleRedirect()) return;
    if (await validateStoredSession()) return;
    const login = document.getElementById("login");
    const app = document.getElementById("app");
    if (login) login.style.display = "flex";
    if (app) app.style.display = "none";
  }

  window.OBTAuth = {
    requireAuth,
    logout,
    doLogin,
    refreshUser,
    getUser: () => currentUser
  };
})();
