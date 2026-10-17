#!/usr/bin/env bash
# Обновление кода из GitHub и перезапуск сборщиков (сделки, стакан). .env и данные базы не трогает.
# Схема базы (schema.sql) применяется только при первом запуске, её правки здесь не накатываются.
set -euo pipefail

main() {
  cd "$(dirname "$0")"
  curl -fsSL -o /tmp/fp.tgz https://github.com/1815071-ship-it/footprint/archive/refs/heads/main.tar.gz
  tar xzf /tmp/fp.tgz --strip-components=1
  sha256sum -c --quiet SHA256SUMS
  echo "файлы целы (sha256 сходится)"
  docker compose build collector book
  # контейнер пересоздаётся, только если его код или настройки изменились
  docker compose up -d db collector book
  # если Grafana уже запущена - применяем её настройки (дашборд из файла она подхватывает сама)
  if [ -n "$(docker compose --profile ui ps -q grafana)" ]; then
    docker compose --profile ui up -d grafana
  fi
  sleep 5
  docker compose --profile ui ps
}

main "$@"
exit
