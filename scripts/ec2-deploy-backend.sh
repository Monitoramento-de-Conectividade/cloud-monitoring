#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/cloud-monitoring}"
BRANCH="${BRANCH:-main}"
REPO_URL="${REPO_URL:-https://github.com/Monitoramento-de-Conectividade/cloud-monitoring.git}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker nao encontrado. Rode scripts/ec2-install-docker.sh primeiro." >&2
  exit 1
fi

if [ ! -d "${APP_DIR}/.git" ]; then
  mkdir -p "${APP_DIR}"
  git clone "${REPO_URL}" "${APP_DIR}"
fi

cd "${APP_DIR}"
git fetch origin

if git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
  git checkout "${BRANCH}"
else
  git checkout -b "${BRANCH}" "origin/${BRANCH}"
fi

git pull --ff-only origin "${BRANCH}"

if [ ! -f .env.backend ]; then
  echo "Arquivo .env.backend nao encontrado em ${APP_DIR}." >&2
  echo "Crie com base em .env.backend.example antes de subir o container." >&2
  exit 1
fi

mkdir -p certs logs_mqtt

docker compose build backend
docker compose up -d backend
docker compose ps backend

echo "Deploy concluido."
