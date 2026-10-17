#!/usr/bin/env bash
# Первый запуск footprint: пароль базы, сборка, старт db + collector.
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -f .env ]; then
  echo "PG_PASSWORD=$(openssl rand -hex 16)" > .env
  chmod 600 .env
  echo "создан .env с паролем базы"
fi
docker compose build collector
docker compose up -d db collector
sleep 5
docker compose ps
echo
echo "логи коллектора (последние 20 строк):"
docker compose logs --tail 20 collector
