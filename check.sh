#!/usr/bin/env bash
# Сверка наших данных с официальными данными бирж: сделки и свечи всех 4 бирж + пульс потоков,
# затем стакан (если сборщик стакана запущен).
cd "$(dirname "$0")"
docker compose exec -T collector python check.py
if [ -n "$(docker compose ps -q book)" ]; then
  docker compose exec -T book python bookcheck.py
fi
