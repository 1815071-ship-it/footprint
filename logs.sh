#!/usr/bin/env bash
# Последние строки лога коллектора (ошибки, статистика раз в минуту).
cd "$(dirname "$0")"
docker compose logs --tail "${1:-30}" collector
