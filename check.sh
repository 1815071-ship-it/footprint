#!/usr/bin/env bash
# Сверка наших данных с официальными данными всех 4 бирж (свечи и лента сделок) + пульс потоков.
cd "$(dirname "$0")"
docker compose exec -T collector python check.py
