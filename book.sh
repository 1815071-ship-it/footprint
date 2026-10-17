#!/usr/bin/env bash
# Стакан: собрать и запустить сборщик стакана, показать память сервера и первые строки лога.
# Сборщик сделок (collector) не перезапускается. Повторный запуск безопасен.
set -euo pipefail

main() {
  cd "$(dirname "$0")"
  echo "память сервера до запуска:"
  free -m
  docker compose build book
  docker compose up -d book
  echo "жду первые снимки (75 с)..."
  sleep 75
  docker compose ps book
  docker compose logs --tail 12 book
  echo
  docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"
}

main "$@"
exit
