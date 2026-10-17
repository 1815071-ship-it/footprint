#!/usr/bin/env bash
# Шаг 5: Grafana с дашбордом footprint. Повторный запуск безопасен (пароли не меняет).
set -euo pipefail

main() {
  cd "$(dirname "$0")"
  # пароли: админ Grafana и read-only пользователь базы; генерируются один раз и живут в .env
  for k in GRAFANA_ADMIN_PASSWORD GRAFANA_DB_PASSWORD; do
    grep -q "^$k=" .env || echo "$k=$(openssl rand -hex 16)" >> .env
  done
  set -a; . ./.env; set +a

  # пользователь базы только для чтения: Grafana смотрит наружу, писать в базу ей незачем
  docker compose exec -T db psql -q -v ON_ERROR_STOP=1 -U fp -d footprint -v pw="$GRAFANA_DB_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE grafana_ro LOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') \gexec
ALTER ROLE grafana_ro PASSWORD :'pw';
ALTER ROLE grafana_ro SET statement_timeout = '20s';
GRANT CONNECT ON DATABASE footprint TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE fp IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
SQL
  echo "пользователь базы grafana_ro готов (только чтение)"

  docker compose --profile ui up -d grafana
  echo "жду запуск Grafana и установку плагина Business Charts..."
  for _ in $(seq 1 30); do
    if docker compose --profile ui exec -T grafana wget -q -O /dev/null http://localhost:3000/api/health; then
      break
    fi
    sleep 3
  done
  docker compose --profile ui ps grafana
  docker compose --profile ui logs --tail 15 grafana
  echo
  echo "Grafana: https://andrey-bridge.duckdns.org/fp/  логин admin, пароль - строка GRAFANA_ADMIN_PASSWORD в /opt/footprint/.env"
}

main "$@"
exit
