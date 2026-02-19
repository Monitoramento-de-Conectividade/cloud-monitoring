#!/usr/bin/env bash
set -euo pipefail

# Configura HTTPS (Nginx + Certbot) para o backend e executa verificacoes basicas.
#
# Uso:
#   DOMAIN=back-cloud-monitor.duckdns.org EMAIL=voce@dominio.com bash scripts/ec2-setup-https.sh
# Ou:
#   bash scripts/ec2-setup-https.sh back-cloud-monitor.duckdns.org voce@dominio.com
#
# Variaveis opcionais:
#   BACKEND_UPSTREAM=http://127.0.0.1:8008
#   HEALTH_PATH=/login
#   FRONTEND_URL=https://cloud-monitoring.vercel.app
#   NGINX_SITE_NAME=cloud-monitoring-backend

DOMAIN="${DOMAIN:-${1:-}}"
EMAIL="${EMAIL:-${2:-}}"
BACKEND_UPSTREAM="${BACKEND_UPSTREAM:-http://127.0.0.1:8008}"
HEALTH_PATH="${HEALTH_PATH:-/login}"
FRONTEND_URL="${FRONTEND_URL:-https://cloud-monitoring.vercel.app}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-cloud-monitoring-backend}"
NGINX_SITE_FILE="/etc/nginx/sites-available/${NGINX_SITE_NAME}"

log() {
  printf '[ec2-setup-https] %s\n' "$*"
}

fail() {
  printf '[ec2-setup-https] ERRO: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || fail "comando obrigatorio ausente: ${cmd}"
}

imds_get() {
  local path="$1"
  local token
  token="$(curl -fsS -m 2 -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)"
  if [ -n "${token}" ]; then
    curl -fsS -m 2 -H "X-aws-ec2-metadata-token: ${token}" "http://169.254.169.254/${path}" || true
  else
    curl -fsS -m 2 "http://169.254.169.254/${path}" || true
  fi
}

if [ -z "${DOMAIN}" ]; then
  fail "defina DOMAIN (ex.: back-cloud-monitor.duckdns.org)."
fi

if [ -z "${EMAIL}" ]; then
  fail "defina EMAIL (ex.: seu-email@dominio.com)."
fi

if [[ "${BACKEND_UPSTREAM}" == */ ]]; then
  BACKEND_UPSTREAM="${BACKEND_UPSTREAM%/}"
fi

if [[ "${HEALTH_PATH}" != /* ]]; then
  HEALTH_PATH="/${HEALTH_PATH}"
fi

require_cmd curl
require_cmd awk
require_cmd grep
require_cmd sed
require_cmd getent
require_cmd systemctl
require_cmd sudo

if ! command -v apt-get >/dev/null 2>&1; then
  fail "este script foi feito para Ubuntu/Debian (apt-get)."
fi

log "Validando resolucao DNS do dominio ${DOMAIN}..."
DOMAIN_IP="$(getent ahostsv4 "${DOMAIN}" | awk 'NR==1 {print $1}')"
[ -n "${DOMAIN_IP}" ] || fail "nao foi possivel resolver ${DOMAIN}."
log "Dominio resolve para: ${DOMAIN_IP}"

PUBLIC_IP="$(imds_get latest/meta-data/public-ipv4)"
if [ -n "${PUBLIC_IP}" ]; then
  log "IP publico da EC2: ${PUBLIC_IP}"
  if [ "${DOMAIN_IP}" != "${PUBLIC_IP}" ]; then
    fail "o dominio ${DOMAIN} nao aponta para esta EC2. Atualize o DNS e tente novamente."
  fi
else
  log "Nao foi possivel ler IP publico via metadata. Seguindo sem esta validacao."
fi

log "Verificando backend local em ${BACKEND_UPSTREAM}${HEALTH_PATH}..."
if ! curl -fsS -m 8 "${BACKEND_UPSTREAM}${HEALTH_PATH}" >/dev/null; then
  fail "backend local nao respondeu. Suba o container antes: sudo docker compose up -d --build backend"
fi
log "Backend local respondeu."

log "Instalando Nginx + Certbot..."
sudo apt-get update -y
sudo apt-get install -y nginx certbot python3-certbot-nginx

log "Configurando Nginx para proxy reverso..."
sudo tee "${NGINX_SITE_FILE}" >/dev/null <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name ${DOMAIN};

    location / {
        proxy_pass ${BACKEND_UPSTREAM};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
EOF

sudo ln -sfn "${NGINX_SITE_FILE}" "/etc/nginx/sites-enabled/${NGINX_SITE_NAME}"
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

log "Verificando Nginx local com Host=${DOMAIN}..."
if ! curl -fsSI -m 10 -H "Host: ${DOMAIN}" "http://127.0.0.1${HEALTH_PATH}" >/dev/null; then
  fail "Nginx nao conseguiu encaminhar para o backend. Revise BACKEND_UPSTREAM (${BACKEND_UPSTREAM})."
fi

log "Emitindo/renovando certificado HTTPS..."
sudo certbot --nginx \
  -d "${DOMAIN}" \
  --non-interactive \
  --agree-tos \
  --email "${EMAIL}" \
  --redirect \
  --keep-until-expiring

sudo systemctl enable certbot.timer >/dev/null 2>&1 || true
sudo systemctl start certbot.timer >/dev/null 2>&1 || true

log "Verificando endpoint HTTPS..."
HTTPS_STATUS="$(curl -s -o /dev/null -w "%{http_code}" -m 10 "https://${DOMAIN}${HEALTH_PATH}")"
if [ "${HTTPS_STATUS}" != "200" ]; then
  fail "HTTPS respondeu status ${HTTPS_STATUS} em https://${DOMAIN}${HEALTH_PATH}."
fi
log "HTTPS OK (status 200)."

log "Verificando preflight CORS para ${FRONTEND_URL}..."
CORS_HEADERS="$(
  curl -sSI -X OPTIONS -m 10 "https://${DOMAIN}/auth/login" \
    -H "Origin: ${FRONTEND_URL}" \
    -H "Access-Control-Request-Method: POST"
)"

CORS_STATUS="$(printf '%s\n' "${CORS_HEADERS}" | awk 'toupper($1) ~ /^HTTP\// {code=$2} END {print code}')"
CORS_ALLOW_ORIGIN="$(printf '%s\n' "${CORS_HEADERS}" | awk -F': ' 'tolower($1)=="access-control-allow-origin" {gsub("\r","",$2); print $2; exit}')"
CORS_ALLOW_CREDENTIALS="$(printf '%s\n' "${CORS_HEADERS}" | awk -F': ' 'tolower($1)=="access-control-allow-credentials" {gsub("\r","",$2); print $2; exit}')"

if [ "${CORS_STATUS}" != "204" ] && [ "${CORS_STATUS}" != "200" ]; then
  fail "preflight CORS falhou (status ${CORS_STATUS}). Verifique CORS_ALLOWED_ORIGINS na .env.backend."
fi

if [ "${CORS_ALLOW_ORIGIN}" != "${FRONTEND_URL}" ]; then
  fail "Access-Control-Allow-Origin (${CORS_ALLOW_ORIGIN}) difere de FRONTEND_URL (${FRONTEND_URL})."
fi

if [ "${CORS_ALLOW_CREDENTIALS}" != "true" ]; then
  fail "Access-Control-Allow-Credentials deve ser true para login com cookie."
fi

log "CORS OK."
log "Concluido com sucesso."
echo
echo "Proximos passos obrigatorios:"
echo "1) Atualize frontend/runtime-config.js para:"
echo "   window.CLOUDV2_API_BASE_URL = \"https://${DOMAIN}\";"
echo "2) No GitHub Secrets, garanta:"
echo "   BACKEND_URL=https://${DOMAIN}"
echo "3) No Security Group da EC2, mantenha apenas:"
echo "   - SSH 22 (seu IP/32)"
echo "   - HTTP 80 (0.0.0.0/0)"
echo "   - HTTPS 443 (0.0.0.0/0)"
