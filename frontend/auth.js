(function () {
  const page = String(document.body.dataset.authPage || "").trim();
  const statusEl = document.getElementById("statusMessage");
  const HAS_DOM = typeof document !== "undefined";
  const HAS_WINDOW = typeof window !== "undefined";

  function resolveApiBaseUrl() {
    if (!HAS_WINDOW) return "";
    const fromGlobal = String(window.CLOUDV2_API_BASE_URL || "").trim();
    if (fromGlobal) {
      return fromGlobal.replace(/\/+$/, "");
    }
    if (!HAS_DOM) return "";
    const meta = document.querySelector('meta[name="cloudv2-api-base-url"]');
    if (!meta) return "";
    const fromMeta = String(meta.getAttribute("content") || "").trim();
    return fromMeta.replace(/\/+$/, "");
  }

  const API_BASE_URL = resolveApiBaseUrl();

  function buildApiUrl(url) {
    const normalized = String(url || "").trim();
    if (!normalized) return normalized;
    if (/^https?:\/\//i.test(normalized)) return normalized;
    if (!normalized.startsWith("/")) return normalized;
    const isApiPath =
      normalized.startsWith("/api/")
      || normalized.startsWith("/auth/")
      || normalized.startsWith("/account/");
    if (!isApiPath || !API_BASE_URL) return normalized;
    return `${API_BASE_URL}${normalized}`;
  }

  function setStatus(message, tone) {
    if (!statusEl) return;
    statusEl.className = "status";
    if (tone === "error") statusEl.classList.add("error");
    if (tone === "warn") statusEl.classList.add("warn");
    if (tone === "success") statusEl.classList.add("success");
    statusEl.textContent = String(message || "").trim() || " ";
  }

  function queryParam(name) {
    const params = new URLSearchParams(window.location.search || "");
    return String(params.get(name) || "").trim();
  }

  function rememberEmail(email) {
    const value = String(email || "").trim().toLowerCase();
    if (!value) return;
    try {
      sessionStorage.setItem("auth_last_email", value);
    } catch (err) {
      // no-op
    }
  }

  function getRememberedEmail() {
    try {
      return String(sessionStorage.getItem("auth_last_email") || "").trim().toLowerCase();
    } catch (err) {
      return "";
    }
  }

  async function safeJson(response) {
    try {
      return await response.json();
    } catch (err) {
      return {};
    }
  }

  async function postJson(url, payload) {
    const response = await fetch(buildApiUrl(url), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload || {}),
    });
    const data = await safeJson(response);
    return { response, data };
  }

  async function getJson(url) {
    const response = await fetch(buildApiUrl(url), {
      method: "GET",
      credentials: "include",
    });
    const data = await safeJson(response);
    return { response, data };
  }

  function downloadJson(filename, payload) {
    const serialized = JSON.stringify(payload || {}, null, 2);
    const blob = new Blob([serialized], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  async function initLoginPage() {
    const form = document.getElementById("loginForm");
    const emailEl = document.getElementById("email");
    const passwordEl = document.getElementById("password");
    if (!form || !emailEl || !passwordEl) return;

    const remembered = getRememberedEmail();
    if (remembered && !emailEl.value) emailEl.value = remembered;

    if (queryParam("verified") === "1") {
      setStatus("E-mail verificado. Agora voce pode fazer login.", "success");
    } else if (queryParam("reset") === "1") {
      setStatus("Senha redefinida. Faca login com a nova senha.", "success");
    } else if (queryParam("deleted") === "1") {
      setStatus("Conta excluida com sucesso.", "success");
    } else {
      setStatus("Informe suas credenciais para acessar o painel.", "warn");
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const email = String(emailEl.value || "").trim().toLowerCase();
      const password = String(passwordEl.value || "");
      rememberEmail(email);
      setStatus("Validando credenciais...", "warn");

      try {
        const { response, data } = await postJson("/auth/login", { email, password });
        if (response.ok && data.ok) {
          window.location.assign(data.redirect || "/index.html");
          return;
        }
        if (String(data.code || "") === "email_not_verified") {
          setStatus("Seu e-mail ainda nao foi verificado. Redirecionando...", "warn");
          window.location.assign(data.redirect || "/verify-email");
          return;
        }
        setStatus(data.message || data.error || "Falha no login.", "error");
      } catch (err) {
        setStatus("Falha de conexao. Tente novamente.", "error");
      }
    });
  }

  async function initRegisterPage() {
    const form = document.getElementById("registerForm");
    const nameEl = document.getElementById("name");
    const emailEl = document.getElementById("email");
    const passwordEl = document.getElementById("password");
    const confirmEl = document.getElementById("passwordConfirm");
    const privacyEl = document.getElementById("privacyAccepted");
    if (!form || !emailEl || !passwordEl || !confirmEl || !privacyEl) return;

    const remembered = getRememberedEmail();
    if (remembered && !emailEl.value) emailEl.value = remembered;
    setStatus("Cadastre e confirme seu e-mail para acessar o sistema.", "warn");

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const payload = {
        name: String((nameEl && nameEl.value) || "").trim(),
        email: String(emailEl.value || "").trim().toLowerCase(),
        password: String(passwordEl.value || ""),
        password_confirm: String(confirmEl.value || ""),
        privacy_policy_accepted: !!privacyEl.checked,
      };
      rememberEmail(payload.email);
      setStatus("Criando cadastro...", "warn");

      try {
        const { response, data } = await postJson("/auth/register", payload);
        if (response.ok && data.ok) {
          const delivery = String(data.email_delivery || "").trim().toLowerCase();
          if (delivery === "smtp_failed") {
            const detail = String(data.email_error || "").trim();
            setStatus(
              (data.message || "Cadastro concluido, mas houve falha no envio do e-mail.")
                + (detail ? ` Detalhe: ${detail}` : ""),
              "error"
            );
            return;
          }
          if (delivery === "console_link") {
            setStatus(
              (data.message || "Cadastro concluido.")
                + " Abra o console do servidor para copiar o link de verificacao.",
              "warn"
            );
            return;
          }
          setStatus(data.message || "Cadastro concluido.", "success");
          window.setTimeout(() => {
            window.location.assign("/verify-email?registered=1");
          }, 600);
          return;
        }
        const details = Array.isArray(data.details) ? ` ${data.details.join(" ")}` : "";
        setStatus((data.message || data.error || "Falha ao cadastrar.") + details, "error");
      } catch (err) {
        setStatus("Falha de conexao. Tente novamente.", "error");
      }
    });
  }

  async function initVerifyEmailPage() {
    const form = document.getElementById("resendForm");
    const emailEl = document.getElementById("email");
    const logoutBtn = document.getElementById("logoutBtn");
    const exportBtn = document.getElementById("exportBtn");
    const deleteBtn = document.getElementById("deleteBtn");
    const accountActions = document.getElementById("accountActions");

    if (!form || !emailEl) return;

    const remembered = getRememberedEmail();
    if (remembered && !emailEl.value) emailEl.value = remembered;

    const status = queryParam("status");
    if (status === "expired") {
      setStatus("O link expirou. Solicite um novo e-mail de verificacao.", "warn");
    } else if (status === "invalid") {
      setStatus("Link invalido. Solicite um novo e-mail de verificacao.", "error");
    } else if (queryParam("registered") === "1") {
      setStatus("Cadastro concluido. Verifique sua caixa de entrada para ativar a conta.", "success");
    } else {
      setStatus("Sua conta ainda nao foi verificada.", "warn");
    }

    let currentUser = null;
    try {
      const { response, data } = await getJson("/auth/me");
      if (response.ok && data.ok && data.authenticated) {
        currentUser = data.user || null;
        if (currentUser && currentUser.email_verified) {
          window.location.assign("/index.html");
          return;
        }
        if (currentUser && currentUser.email) {
          emailEl.value = String(currentUser.email).trim().toLowerCase();
          rememberEmail(emailEl.value);
        }
      }
    } catch (err) {
      // no-op
    }

    if (currentUser && accountActions) {
      accountActions.classList.remove("hidden");
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const email = String(emailEl.value || "").trim().toLowerCase();
      if (email) rememberEmail(email);
      setStatus("Solicitando envio de verificacao...", "warn");
      try {
        const { data } = await postJson("/auth/resend-verification", { email });
        const delivery = String(data.email_delivery || "").trim().toLowerCase();
        if (delivery === "smtp_failed") {
          const detail = String(data.email_error || "").trim();
          setStatus(
            (data.message || "Falha ao enviar e-mail de verificacao.")
              + (detail ? ` Detalhe: ${detail}` : ""),
            "error"
          );
          return;
        }
        if (delivery === "console_link") {
          setStatus(
            (data.message || "Solicitacao aceita.")
              + " Em modo dev, o link esta no console do servidor.",
            "warn"
          );
          return;
        }
        setStatus(data.message || "Se o e-mail existir, enviaremos o link.", "success");
      } catch (err) {
        setStatus("Falha de conexao. Tente novamente.", "error");
      }
    });

    if (logoutBtn) {
      logoutBtn.addEventListener("click", async () => {
        try {
          await postJson("/auth/logout", {});
        } catch (err) {
          // no-op
        }
        window.location.assign("/login");
      });
    }

    if (exportBtn) {
      exportBtn.addEventListener("click", async () => {
        try {
          const { response, data } = await getJson("/account/export");
          if (!response.ok || !data.ok) {
            setStatus(data.message || data.error || "Falha ao exportar dados.", "error");
            return;
          }
          downloadJson("account-export.json", data.data || {});
          setStatus("Exportacao concluida.", "success");
        } catch (err) {
          setStatus("Falha ao exportar dados.", "error");
        }
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener("click", async () => {
        const confirmed = window.confirm("Deseja realmente excluir sua conta?");
        if (!confirmed) return;
        setStatus("Excluindo conta...", "warn");
        try {
          const { response, data } = await postJson("/account/delete", {});
          if (!response.ok || !data.ok) {
            setStatus(data.message || data.error || "Falha ao excluir conta.", "error");
            return;
          }
          window.location.assign("/login?deleted=1");
        } catch (err) {
          setStatus("Falha ao excluir conta.", "error");
        }
      });
    }
  }

  async function initForgotPasswordPage() {
    const form = document.getElementById("forgotForm");
    const emailEl = document.getElementById("email");
    if (!form || !emailEl) return;

    const remembered = getRememberedEmail();
    if (remembered && !emailEl.value) emailEl.value = remembered;
    setStatus("Informe seu e-mail para receber o link de redefinicao.", "warn");

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const email = String(emailEl.value || "").trim().toLowerCase();
      rememberEmail(email);
      setStatus("Solicitando redefinicao...", "warn");
      try {
        const { data } = await postJson("/auth/forgot-password", { email });
        const delivery = String(data.email_delivery || "").trim().toLowerCase();
        if (delivery === "smtp_failed") {
          const detail = String(data.email_error || "").trim();
          setStatus(
            (data.message || "Falha ao enviar e-mail de redefinicao.")
              + (detail ? ` Detalhe: ${detail}` : ""),
            "error"
          );
          return;
        }
        if (delivery === "console_link") {
          setStatus(
            (data.message || "Solicitacao aceita.")
              + " Em modo dev, o link esta no console do servidor.",
            "warn"
          );
          return;
        }
        setStatus(data.message || "Se o e-mail existir, enviaremos o link.", "success");
      } catch (err) {
        setStatus("Falha de conexao. Tente novamente.", "error");
      }
    });
  }

  async function initResetPasswordPage() {
    const form = document.getElementById("resetForm");
    const passwordEl = document.getElementById("password");
    const confirmEl = document.getElementById("passwordConfirm");
    if (!form || !passwordEl || !confirmEl) return;

    const token = queryParam("token");
    if (!token) {
      setStatus("Token ausente. Solicite um novo link de redefinicao.", "error");
      form.querySelectorAll("input,button").forEach((item) => {
        item.disabled = true;
      });
      return;
    }

    setStatus("Defina sua nova senha.", "warn");
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      setStatus("Atualizando senha...", "warn");
      try {
        const { response, data } = await postJson("/auth/reset-password", {
          token,
          password: String(passwordEl.value || ""),
          password_confirm: String(confirmEl.value || ""),
        });
        if (response.ok && data.ok) {
          setStatus(data.message || "Senha redefinida com sucesso.", "success");
          window.setTimeout(() => {
            window.location.assign("/login?reset=1");
          }, 600);
          return;
        }
        const details = Array.isArray(data.details) ? ` ${data.details.join(" ")}` : "";
        setStatus((data.message || data.error || "Falha ao redefinir senha.") + details, "error");
      } catch (err) {
        setStatus("Falha de conexao. Tente novamente.", "error");
      }
    });
  }

  if (page === "login") initLoginPage();
  if (page === "register") initRegisterPage();
  if (page === "verify-email") initVerifyEmailPage();
  if (page === "forgot-password") initForgotPasswordPage();
  if (page === "reset-password") initResetPasswordPage();
})();
