#!/usr/bin/env bash
# Сверка наших данных с официальными свечами Binance + пульс всех бирж.
cd "$(dirname "$0")"
docker compose exec -T collector python check.py
