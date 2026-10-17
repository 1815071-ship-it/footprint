#!/usr/bin/env bash
# Открывает Grafana наружу через Caddy моста: https://andrey-bridge.duckdns.org/fp/
# Меняет ТОЛЬКО /root/bridge/Caddyfile (сначала делает резервную копию) и мягко перезагружает Caddy:
# контейнеры моста не перезапускаются, /webhook и /health продолжают работать.
# Если новый конфиг не проходит проверку или /health моста начинает отвечать иначе - откатывает сам.
set -euo pipefail

main() {
  cd "$(dirname "$0")"
  local cf=/root/bridge/Caddyfile
  local bak="/root/bridge/Caddyfile.bak-$(date +%Y%m%d-%H%M%S)"
  local url=https://andrey-bridge.duckdns.org
  cp -p "$cf" "$bak"
  echo "резервная копия: $bak"
  local before after fp
  before=$(curl -s -o /dev/null -w '%{http_code}' "$url/health" || true)

  # пишем в тот же файл (тот же inode): Caddyfile примонтирован в контейнер одним файлом
  cat deploy/Caddyfile.bridge > "$cf"
  if ! docker exec bridge-caddy-1 caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile; then
    cat "$bak" > "$cf"
    echo "ОШИБКА: новый конфиг не прошёл проверку, старый возвращён. Caddy не перезагружался."
    exit 1
  fi
  docker exec bridge-caddy-1 caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
  sleep 3
  after=$(curl -s -o /dev/null -w '%{http_code}' "$url/health" || true)
  fp=$(curl -s -o /dev/null -w '%{http_code}' "$url/fp/login" || true)
  echo "мост /health: до $before, после $after. Grafana /fp/login: $fp"
  if [ "$after" != "$before" ]; then
    cat "$bak" > "$cf"
    docker exec bridge-caddy-1 caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
    echo "ОШИБКА: /health моста стал отвечать иначе - Caddyfile откатан на резервную копию."
    exit 1
  fi
  echo "готово: $url/fp/"
}

main "$@"
exit
