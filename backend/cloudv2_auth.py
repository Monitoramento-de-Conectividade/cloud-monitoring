"""Authentication and account helpers for the dashboard server."""

import base64
import hashlib
import hmac
import os
import re
import secrets
import smtplib
import sqlite3
import threading
import time
import uuid
from email.message import EmailMessage
from urllib.parse import quote


def _env_int(name, default, minimum=1):
    raw_value = os.environ.get(name)
    try:
        parsed = int(raw_value) if raw_value is not None else int(default)
    except (TypeError, ValueError):
        parsed = int(default)
    if parsed < int(minimum):
        return int(minimum)
    return parsed


SESSION_COOKIE_NAME = str(os.environ.get("AUTH_SESSION_COOKIE_NAME", "cloudv2_session")).strip() or "cloudv2_session"
SESSION_TTL_SEC = _env_int("AUTH_SESSION_TTL_SEC", 12 * 3600, minimum=300)
VERIFY_TOKEN_TTL_SEC = _env_int("AUTH_VERIFY_TOKEN_TTL_SEC", 24 * 3600, minimum=300)
RESET_TOKEN_TTL_SEC = _env_int("AUTH_RESET_TOKEN_TTL_SEC", 3600, minimum=300)
PASSWORD_MIN_LENGTH = _env_int("AUTH_PASSWORD_MIN_LENGTH", 8, minimum=8)
PRIVACY_POLICY_VERSION = str(os.environ.get("PRIVACY_POLICY_VERSION", "2026-02-10")).strip() or "2026-02-10"
TOKEN_PEPPER = str(os.environ.get("AUTH_TOKEN_PEPPER", "cloudv2-dev-token-pepper")).strip() or "cloudv2-dev-token-pepper"


def mask_email(email):
    value = str(email or "").strip()
    if "@" not in value:
        return "***"
    local, _, domain = value.partition("@")
    if not local:
        local_mask = "*"
    elif len(local) == 1:
        local_mask = local[0] + "*"
    elif len(local) == 2:
        local_mask = local[0] + "*"
    else:
        local_mask = f"{local[0]}{'*' * (len(local) - 2)}{local[-1]}"

    domain_parts = [part for part in domain.split(".") if part]
    if not domain_parts:
        return f"{local_mask}@***"

    masked_parts = []
    for part in domain_parts:
        if len(part) <= 2:
            masked_parts.append(part[0] + "*")
        else:
            masked_parts.append(f"{part[0]}{'*' * (len(part) - 2)}{part[-1]}")
    return f"{local_mask}@{'.'.join(masked_parts)}"


def _now_ts():
    return float(time.time())


class InMemoryRateLimiter:
    def __init__(self):
        self._lock = threading.Lock()
        self._history = {}

    def allow(self, scope, key, limit, window_sec):
        safe_scope = str(scope or "").strip() or "default"
        safe_key = str(key or "").strip() or "anonymous"
        safe_limit = max(1, int(limit or 1))
        safe_window = max(1, int(window_sec or 1))
        now = _now_ts()
        bucket_key = (safe_scope, safe_key)

        with self._lock:
            recent = [ts for ts in self._history.get(bucket_key, []) if ts >= (now - safe_window)]
            if len(recent) >= safe_limit:
                retry_after = max(1, int((recent[0] + safe_window) - now))
                self._history[bucket_key] = recent
                return False, retry_after

            recent.append(now)
            self._history[bucket_key] = recent

            if len(self._history) > 3000:
                oldest_cutoff = now - (safe_window * 3)
                for existing_key in list(self._history.keys()):
                    filtered = [ts for ts in self._history.get(existing_key, []) if ts >= oldest_cutoff]
                    if filtered:
                        self._history[existing_key] = filtered
                    else:
                        self._history.pop(existing_key, None)

        return True, 0


class AuthEmailService:
    def __init__(self, logger=None):
        self.logger = logger
        self.mode = self._resolve_mode()
        self.smtp_host = str(os.environ.get("AUTH_SMTP_HOST", "")).strip()
        self.smtp_port = _env_int("AUTH_SMTP_PORT", 587, minimum=1)
        self.smtp_user = str(os.environ.get("AUTH_SMTP_USER", "")).strip()
        self.smtp_password = str(os.environ.get("AUTH_SMTP_PASSWORD", "")).strip()
        self.smtp_from = str(os.environ.get("AUTH_SMTP_FROM", "no-reply@cloud-monitor.local")).strip()
        self.smtp_starttls = str(os.environ.get("AUTH_SMTP_STARTTLS", "1")).strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )

    def _resolve_mode(self):
        forced = str(os.environ.get("AUTH_EMAIL_MODE", "")).strip().lower()
        if forced in ("smtp", "console"):
            return forced
        return "smtp" if str(os.environ.get("AUTH_SMTP_HOST", "")).strip() else "console"

    def _log_dev_link(self, email, label, link):
        masked = mask_email(email)
        print(f"[auth-email:{label}] to={masked} link={link}")

    def _send_smtp(self, email, subject, body):
        if not self.smtp_host:
            raise RuntimeError("AUTH_SMTP_HOST ausente para envio SMTP")

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.smtp_from
        message["To"] = str(email).strip()
        message.set_content(body)

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=15) as server:
            if self.smtp_starttls:
                server.starttls()
            if self.smtp_user:
                server.login(self.smtp_user, self.smtp_password)
            server.send_message(message)

    def send_verification_email(self, email, verification_link):
        subject = "Verificacao de e-mail - Cloud Monitoring"
        body = (
            "Ola,\n\n"
            "Para confirmar seu cadastro, use o link abaixo:\n"
            f"{verification_link}\n\n"
            f"Este link expira em {int(VERIFY_TOKEN_TTL_SEC / 60)} minuto(s).\n"
            "Se voce nao solicitou este cadastro, ignore esta mensagem.\n"
        )
        self._dispatch_email(email, subject, body, "verify", verification_link)

    def send_reset_password_email(self, email, reset_link):
        subject = "Redefinicao de senha - Cloud Monitoring"
        body = (
            "Ola,\n\n"
            "Para redefinir sua senha, use o link abaixo:\n"
            f"{reset_link}\n\n"
            f"Este link expira em {int(RESET_TOKEN_TTL_SEC / 60)} minuto(s).\n"
            "Se voce nao solicitou redefinicao, ignore esta mensagem.\n"
        )
        self._dispatch_email(email, subject, body, "reset", reset_link)

    def _dispatch_email(self, email, subject, body, label, fallback_link):
        if self.mode == "smtp":
            self._send_smtp(email, subject, body)
            return
        self._log_dev_link(email, label, fallback_link)


class AuthService:
    def __init__(self, db_path, logger=None, email_service=None):
        self.db_path = str(db_path or "").strip()
        self.logger = logger
        self.email_service = email_service or AuthEmailService(logger=logger)

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=3.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 3000")
        return conn

    def _normalize_email(self, email):
        return str(email or "").strip().lower()

    def _normalize_name(self, name):
        normalized = str(name or "").strip()
        if not normalized:
            return None
        if len(normalized) > 120:
            return normalized[:120]
        return normalized

    def _is_valid_email(self, email):
        if not email or len(email) > 254:
            return False
        return bool(re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", email))

    def _validate_password_strength(self, password):
        text = str(password or "")
        issues = []
        if len(text) < PASSWORD_MIN_LENGTH:
            issues.append(f"A senha deve ter no minimo {PASSWORD_MIN_LENGTH} caracteres.")
        if not re.search(r"[a-z]", text):
            issues.append("A senha deve conter ao menos uma letra minuscula.")
        if not re.search(r"[A-Z]", text):
            issues.append("A senha deve conter ao menos uma letra maiuscula.")
        if not re.search(r"\d", text):
            issues.append("A senha deve conter ao menos um numero.")
        return issues

    def _hash_password(self, password):
        plain = str(password or "")
        salt = secrets.token_bytes(16)
        n = 2**14
        r = 8
        p = 1
        digest = hashlib.scrypt(plain.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=64)
        encoded_salt = base64.urlsafe_b64encode(salt).decode("ascii")
        encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii")
        return f"scrypt${n}${r}${p}${encoded_salt}${encoded_digest}"

    def _verify_password(self, password, stored_hash):
        plain = str(password or "")
        encoded = str(stored_hash or "")
        try:
            algorithm, n_str, r_str, p_str, encoded_salt, encoded_digest = encoded.split("$", 5)
            if algorithm != "scrypt":
                return False
            n = int(n_str)
            r = int(r_str)
            p = int(p_str)
            salt = base64.urlsafe_b64decode(encoded_salt.encode("ascii"))
            expected_digest = base64.urlsafe_b64decode(encoded_digest.encode("ascii"))
            computed_digest = hashlib.scrypt(
                plain.encode("utf-8"),
                salt=salt,
                n=n,
                r=r,
                p=p,
                dklen=len(expected_digest),
            )
            return hmac.compare_digest(computed_digest, expected_digest)
        except Exception:
            return False

    def _hash_secret(self, raw_value, purpose):
        payload = f"{purpose}:{raw_value}:{TOKEN_PEPPER}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _generate_token(self):
        return secrets.token_urlsafe(48)

    def _cleanup_expired_records(self, conn, now_ts):
        cutoff_tokens = float(now_ts) - (30 * 24 * 3600)
        cutoff_sessions = float(now_ts) - (30 * 24 * 3600)
        with conn:
            conn.execute(
                """
                DELETE FROM user_tokens
                WHERE expires_at < ?
                    AND (used_at IS NOT NULL OR expires_at < ?)
                """,
                (float(now_ts), cutoff_tokens),
            )
            conn.execute(
                """
                DELETE FROM user_sessions
                WHERE expires_at < ?
                    AND (revoked_at IS NOT NULL OR expires_at < ?)
                """,
                (float(now_ts), cutoff_sessions),
            )

    def _get_base_url(self, request_base_url):
        env_base = str(os.environ.get("AUTH_BASE_URL", "")).strip()
        if env_base:
            return env_base.rstrip("/")

        render_base = str(os.environ.get("RENDER_EXTERNAL_URL", "")).strip()
        if render_base:
            return render_base.rstrip("/")

        fallback = str(request_base_url or "").strip()
        if fallback:
            return fallback.rstrip("/")
        return "http://localhost:8008"

    def _build_verify_link(self, request_base_url, token):
        base = self._get_base_url(request_base_url)
        return f"{base}/auth/verify?token={quote(str(token or ''), safe='')}"

    def _build_reset_link(self, request_base_url, token):
        base = self._get_base_url(request_base_url)
        return f"{base}/reset-password?token={quote(str(token or ''), safe='')}"

    def _invalidate_token_type(self, conn, user_id, token_type, now_ts):
        conn.execute(
            """
            UPDATE user_tokens
            SET used_at = COALESCE(used_at, ?)
            WHERE user_id = ?
                AND type = ?
                AND used_at IS NULL
            """,
            (float(now_ts), str(user_id), str(token_type)),
        )

    def _insert_token(self, conn, user_id, token_type, ttl_sec, now_ts):
        raw_token = self._generate_token()
        token_hash = self._hash_secret(raw_token, f"token:{token_type}")
        conn.execute(
            """
            INSERT INTO user_tokens (
                id,
                user_id,
                type,
                token_hash,
                expires_at,
                used_at,
                created_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?)
            """,
            (
                str(uuid.uuid4()),
                str(user_id),
                str(token_type),
                str(token_hash),
                float(now_ts + ttl_sec),
                float(now_ts),
            ),
        )
        return raw_token

    def _insert_session(self, conn, user_id, ip_address, user_agent, now_ts):
        raw_session = self._generate_token()
        session_hash = self._hash_secret(raw_session, "session")
        ip_hash = None
        if str(ip_address or "").strip():
            ip_hash = self._hash_secret(str(ip_address or "").strip(), "ip")

        ua = str(user_agent or "").strip()
        if len(ua) > 255:
            ua = ua[:255]

        conn.execute(
            """
            INSERT INTO user_sessions (
                id,
                user_id,
                session_hash,
                created_at,
                expires_at,
                revoked_at,
                last_seen_at,
                user_agent,
                ip_hash
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                str(user_id),
                str(session_hash),
                float(now_ts),
                float(now_ts + SESSION_TTL_SEC),
                float(now_ts),
                ua or None,
                ip_hash,
            ),
        )
        return raw_session

    def _public_user_from_row(self, row):
        if row is None:
            return None
        email = str(row["email"] or "").strip().lower()
        return {
            "id": str(row["id"]),
            "email": email,
            "email_masked": mask_email(email),
            "name": str(row["name"] or ""),
            "status": str(row["status"] or "active"),
            "email_verified": row["email_verified_at"] is not None,
            "email_verified_at": row["email_verified_at"],
            "privacy_policy_version": str(row["privacy_policy_version"] or ""),
            "privacy_policy_accepted_at": row["privacy_policy_accepted_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_login_at": row["last_login_at"],
            "deleted_at": row["deleted_at"],
        }

    def register_user(
        self,
        email,
        password,
        password_confirm,
        name=None,
        privacy_policy_accepted=False,
        request_base_url="",
    ):
        normalized_email = self._normalize_email(email)
        normalized_name = self._normalize_name(name)
        plain_password = str(password or "")
        plain_confirm = str(password_confirm or "")

        if not self._is_valid_email(normalized_email):
            return {"ok": False, "code": "invalid_email", "message": "E-mail invalido."}

        if plain_password != plain_confirm:
            return {"ok": False, "code": "password_mismatch", "message": "As senhas nao conferem."}

        password_issues = self._validate_password_strength(plain_password)
        if password_issues:
            return {
                "ok": False,
                "code": "weak_password",
                "message": "Senha fora do padrao minimo de seguranca.",
                "details": password_issues,
            }

        if not bool(privacy_policy_accepted):
            return {
                "ok": False,
                "code": "privacy_policy_required",
                "message": "E necessario aceitar a Politica de Privacidade para continuar.",
            }

        now_ts = _now_ts()
        password_hash = self._hash_password(plain_password)
        verify_token = None
        target_email = normalized_email

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            existing = conn.execute(
                """
                SELECT *
                FROM users
                WHERE email = ?
                LIMIT 1
                """,
                (normalized_email,),
            ).fetchone()

            with conn:
                if existing is not None and existing["deleted_at"] is None:
                    return {
                        "ok": False,
                        "code": "email_in_use",
                        "message": "E-mail ja cadastrado.",
                    }

                if existing is not None and existing["deleted_at"] is not None:
                    user_id = str(existing["id"])
                    conn.execute(
                        """
                        UPDATE users
                        SET
                            name = ?,
                            password_hash = ?,
                            email_verified_at = NULL,
                            status = 'active',
                            privacy_policy_version = ?,
                            privacy_policy_accepted_at = ?,
                            updated_at = ?,
                            last_login_at = NULL,
                            deleted_at = NULL
                        WHERE id = ?
                        """,
                        (
                            normalized_name,
                            password_hash,
                            PRIVACY_POLICY_VERSION,
                            now_ts,
                            now_ts,
                            user_id,
                        ),
                    )
                else:
                    user_id = str(uuid.uuid4())
                    conn.execute(
                        """
                        INSERT INTO users (
                            id,
                            email,
                            name,
                            password_hash,
                            email_verified_at,
                            status,
                            privacy_policy_version,
                            privacy_policy_accepted_at,
                            created_at,
                            updated_at,
                            last_login_at,
                            deleted_at
                        ) VALUES (?, ?, ?, ?, NULL, 'active', ?, ?, ?, ?, NULL, NULL)
                        """,
                        (
                            user_id,
                            normalized_email,
                            normalized_name,
                            password_hash,
                            PRIVACY_POLICY_VERSION,
                            now_ts,
                            now_ts,
                            now_ts,
                        ),
                    )

                self._invalidate_token_type(conn, user_id, "email_verify", now_ts)
                verify_token = self._insert_token(conn, user_id, "email_verify", VERIFY_TOKEN_TTL_SEC, now_ts)

        email_sent = True
        try:
            verify_link = self._build_verify_link(request_base_url, verify_token)
            self.email_service.send_verification_email(target_email, verify_link)
        except Exception:
            email_sent = False

        return {
            "ok": True,
            "code": "registered",
            "message": (
                "Cadastro concluido. "
                + (
                    "Enviamos um link de verificacao para o e-mail informado."
                    if email_sent
                    else "Nao foi possivel enviar o e-mail agora; tente reenviar a verificacao."
                )
            ),
            "email_masked": mask_email(target_email),
            "privacy_policy_version": PRIVACY_POLICY_VERSION,
        }

    def login_user(self, email, password, ip_address=None, user_agent=None):
        normalized_email = self._normalize_email(email)
        plain_password = str(password or "")
        now_ts = _now_ts()

        if not self._is_valid_email(normalized_email) or not plain_password:
            return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            user_row = conn.execute(
                """
                SELECT *
                FROM users
                WHERE email = ?
                LIMIT 1
                """,
                (normalized_email,),
            ).fetchone()

            if user_row is None:
                return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

            if user_row["deleted_at"] is not None or str(user_row["status"] or "active") != "active":
                return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

            if not self._verify_password(plain_password, user_row["password_hash"]):
                return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

            if user_row["email_verified_at"] is None:
                return {
                    "ok": False,
                    "code": "email_not_verified",
                    "message": "E-mail ainda nao verificado.",
                    "email_masked": mask_email(normalized_email),
                }

            with conn:
                session_token = self._insert_session(
                    conn,
                    user_id=str(user_row["id"]),
                    ip_address=ip_address,
                    user_agent=user_agent,
                    now_ts=now_ts,
                )
                conn.execute(
                    """
                    UPDATE users
                    SET
                        last_login_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now_ts, now_ts, str(user_row["id"])),
                )
                refreshed_row = conn.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (str(user_row["id"]),),
                ).fetchone()

        return {
            "ok": True,
            "code": "authenticated",
            "message": "Login realizado com sucesso.",
            "session_token": session_token,
            "session_ttl_sec": SESSION_TTL_SEC,
            "user": self._public_user_from_row(refreshed_row),
        }

    def resolve_session(self, raw_session_token, touch=True):
        token = str(raw_session_token or "").strip()
        if not token:
            return None

        now_ts = _now_ts()
        session_hash = self._hash_secret(token, "session")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    sessions.id AS session_id,
                    sessions.user_id AS session_user_id,
                    sessions.expires_at AS session_expires_at,
                    users.*
                FROM user_sessions AS sessions
                JOIN users
                    ON users.id = sessions.user_id
                WHERE sessions.session_hash = ?
                    AND sessions.revoked_at IS NULL
                    AND sessions.expires_at > ?
                    AND users.deleted_at IS NULL
                    AND users.status = 'active'
                LIMIT 1
                """,
                (session_hash, now_ts),
            ).fetchone()

            if row is None:
                return None

            if touch:
                with conn:
                    conn.execute(
                        """
                        UPDATE user_sessions
                        SET last_seen_at = ?
                        WHERE id = ?
                        """,
                        (now_ts, str(row["session_id"])),
                    )

        user = self._public_user_from_row(row)
        if user is None:
            return None

        return {
            "session_id": str(row["session_id"]),
            "session_user_id": str(row["session_user_id"]),
            "session_expires_at": row["session_expires_at"],
            "user": user,
        }

    def logout_session(self, raw_session_token):
        token = str(raw_session_token or "").strip()
        if not token:
            return
        now_ts = _now_ts()
        session_hash = self._hash_secret(token, "session")
        with self._connect() as conn:
            with conn:
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = COALESCE(revoked_at, ?)
                    WHERE session_hash = ?
                    """,
                    (now_ts, session_hash),
                )

    def verify_email_token(self, raw_token):
        token = str(raw_token or "").strip()
        if not token:
            return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

        now_ts = _now_ts()
        token_hash = self._hash_secret(token, "token:email_verify")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    user_tokens.id AS token_id,
                    user_tokens.user_id AS token_user_id,
                    user_tokens.expires_at AS token_expires_at,
                    user_tokens.used_at AS token_used_at,
                    users.*
                FROM user_tokens
                JOIN users
                    ON users.id = user_tokens.user_id
                WHERE user_tokens.type = 'email_verify'
                    AND user_tokens.token_hash = ?
                LIMIT 1
                """,
                (token_hash,),
            ).fetchone()

            if row is None:
                return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

            if row["token_used_at"] is not None:
                return {"ok": False, "code": "token_used", "message": "Token ja utilizado."}

            if float(row["token_expires_at"]) < now_ts:
                with conn:
                    conn.execute(
                        """
                        UPDATE user_tokens
                        SET used_at = COALESCE(used_at, ?)
                        WHERE id = ?
                        """,
                        (now_ts, str(row["token_id"])),
                    )
                return {"ok": False, "code": "token_expired", "message": "Token expirado."}

            if row["deleted_at"] is not None or str(row["status"] or "active") != "active":
                return {"ok": False, "code": "invalid_user", "message": "Usuario indisponivel."}

            with conn:
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = ?
                    WHERE id = ?
                        AND used_at IS NULL
                    """,
                    (now_ts, str(row["token_id"])),
                )
                conn.execute(
                    """
                    UPDATE users
                    SET
                        email_verified_at = COALESCE(email_verified_at, ?),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now_ts, now_ts, str(row["token_user_id"])),
                )

        return {"ok": True, "code": "verified", "message": "E-mail verificado com sucesso."}

    def resend_verification(self, email=None, user_id=None, request_base_url=""):
        now_ts = _now_ts()
        normalized_email = self._normalize_email(email)
        target_email = None
        verify_token = None

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            if str(user_id or "").strip():
                row = conn.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (str(user_id),),
                ).fetchone()
            elif self._is_valid_email(normalized_email):
                row = conn.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE email = ?
                    LIMIT 1
                    """,
                    (normalized_email,),
                ).fetchone()
            else:
                row = None

            if (
                row is not None
                and row["deleted_at"] is None
                and str(row["status"] or "active") == "active"
                and row["email_verified_at"] is None
            ):
                target_email = str(row["email"] or "").strip().lower()
                with conn:
                    self._invalidate_token_type(conn, str(row["id"]), "email_verify", now_ts)
                    verify_token = self._insert_token(conn, str(row["id"]), "email_verify", VERIFY_TOKEN_TTL_SEC, now_ts)

        if target_email and verify_token:
            try:
                verify_link = self._build_verify_link(request_base_url, verify_token)
                self.email_service.send_verification_email(target_email, verify_link)
            except Exception:
                pass

        return {
            "ok": True,
            "code": "accepted",
            "message": "Se o e-mail existir, enviaremos um link de verificacao.",
        }

    def forgot_password(self, email, request_base_url=""):
        now_ts = _now_ts()
        normalized_email = self._normalize_email(email)
        target_email = None
        reset_token = None

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            if self._is_valid_email(normalized_email):
                row = conn.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE email = ?
                    LIMIT 1
                    """,
                    (normalized_email,),
                ).fetchone()
            else:
                row = None

            if (
                row is not None
                and row["deleted_at"] is None
                and str(row["status"] or "active") == "active"
                and row["email_verified_at"] is not None
            ):
                target_email = str(row["email"] or "").strip().lower()
                with conn:
                    self._invalidate_token_type(conn, str(row["id"]), "password_reset", now_ts)
                    reset_token = self._insert_token(conn, str(row["id"]), "password_reset", RESET_TOKEN_TTL_SEC, now_ts)

        if target_email and reset_token:
            try:
                reset_link = self._build_reset_link(request_base_url, reset_token)
                self.email_service.send_reset_password_email(target_email, reset_link)
            except Exception:
                pass

        return {
            "ok": True,
            "code": "accepted",
            "message": "Se o e-mail existir, enviaremos um link de redefinicao de senha.",
        }

    def reset_password(self, raw_token, new_password, confirm_password):
        token = str(raw_token or "").strip()
        new_plain = str(new_password or "")
        confirm_plain = str(confirm_password or "")

        if not token:
            return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

        if new_plain != confirm_plain:
            return {"ok": False, "code": "password_mismatch", "message": "As senhas nao conferem."}

        password_issues = self._validate_password_strength(new_plain)
        if password_issues:
            return {
                "ok": False,
                "code": "weak_password",
                "message": "Senha fora do padrao minimo de seguranca.",
                "details": password_issues,
            }

        now_ts = _now_ts()
        token_hash = self._hash_secret(token, "token:password_reset")
        new_password_hash = self._hash_password(new_plain)

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            row = conn.execute(
                """
                SELECT
                    user_tokens.id AS token_id,
                    user_tokens.user_id AS token_user_id,
                    user_tokens.expires_at AS token_expires_at,
                    user_tokens.used_at AS token_used_at,
                    users.*
                FROM user_tokens
                JOIN users
                    ON users.id = user_tokens.user_id
                WHERE user_tokens.type = 'password_reset'
                    AND user_tokens.token_hash = ?
                LIMIT 1
                """,
                (token_hash,),
            ).fetchone()

            if row is None:
                return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

            if row["token_used_at"] is not None:
                return {"ok": False, "code": "token_used", "message": "Token ja utilizado."}

            if float(row["token_expires_at"]) < now_ts:
                with conn:
                    conn.execute(
                        """
                        UPDATE user_tokens
                        SET used_at = COALESCE(used_at, ?)
                        WHERE id = ?
                        """,
                        (now_ts, str(row["token_id"])),
                    )
                return {"ok": False, "code": "token_expired", "message": "Token expirado."}

            if row["deleted_at"] is not None or str(row["status"] or "active") != "active":
                return {"ok": False, "code": "invalid_user", "message": "Usuario indisponivel."}

            user_id = str(row["token_user_id"])
            with conn:
                conn.execute(
                    """
                    UPDATE users
                    SET
                        password_hash = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (new_password_hash, now_ts, user_id),
                )
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE user_id = ?
                        AND type = 'password_reset'
                        AND used_at IS NULL
                    """,
                    (now_ts, user_id),
                )
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = COALESCE(revoked_at, ?)
                    WHERE user_id = ?
                        AND revoked_at IS NULL
                    """,
                    (now_ts, user_id),
                )

        return {"ok": True, "code": "password_reset", "message": "Senha redefinida com sucesso."}

    def export_account_data(self, user_id):
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return None
        now_ts = _now_ts()

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_user_id,),
            ).fetchone()
            if row is None:
                return None

            token_rows = conn.execute(
                """
                SELECT type, COUNT(*) AS qty
                FROM user_tokens
                WHERE user_id = ?
                    AND used_at IS NULL
                    AND expires_at > ?
                GROUP BY type
                """,
                (normalized_user_id, now_ts),
            ).fetchall()
            active_tokens = {str(item["type"]): int(item["qty"] or 0) for item in token_rows}

            active_sessions_row = conn.execute(
                """
                SELECT COUNT(*) AS qty
                FROM user_sessions
                WHERE user_id = ?
                    AND revoked_at IS NULL
                    AND expires_at > ?
                """,
                (normalized_user_id, now_ts),
            ).fetchone()
            active_sessions = int((active_sessions_row or {"qty": 0})["qty"] or 0)

        user_data = self._public_user_from_row(row)
        if user_data is None:
            return None

        return {
            "exported_at": now_ts,
            "user": user_data,
            "active_sessions": active_sessions,
            "active_tokens": {
                "email_verify": int(active_tokens.get("email_verify", 0)),
                "password_reset": int(active_tokens.get("password_reset", 0)),
            },
            "retention": {
                "verify_token_ttl_sec": VERIFY_TOKEN_TTL_SEC,
                "reset_token_ttl_sec": RESET_TOKEN_TTL_SEC,
                "session_ttl_sec": SESSION_TTL_SEC,
                "logs_policy": "Apenas logs operacionais minimos, sem senha, token bruto ou e-mail completo.",
            },
        }

    def delete_account(self, user_id):
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False
        now_ts = _now_ts()

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_user_id,),
            ).fetchone()
            if row is None:
                return False

            anonymized_email = f"deleted-{normalized_user_id[:8]}-{int(now_ts)}-{uuid.uuid4().hex[:8]}@anon.invalid"
            anonymized_password = self._hash_password(secrets.token_urlsafe(24))
            with conn:
                conn.execute(
                    """
                    UPDATE users
                    SET
                        email = ?,
                        name = NULL,
                        password_hash = ?,
                        email_verified_at = NULL,
                        status = 'disabled',
                        privacy_policy_accepted_at = NULL,
                        updated_at = ?,
                        deleted_at = ?
                    WHERE id = ?
                    """,
                    (
                        anonymized_email,
                        anonymized_password,
                        now_ts,
                        now_ts,
                        normalized_user_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE user_id = ?
                        AND used_at IS NULL
                    """,
                    (now_ts, normalized_user_id),
                )
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = COALESCE(revoked_at, ?)
                    WHERE user_id = ?
                        AND revoked_at IS NULL
                    """,
                    (now_ts, normalized_user_id),
                )

        return True
