# Footprint

Живые кластеры Sell / Buy / Delta по BTC и ETH: Binance, Bybit, OKX (spot и perp), Coinbase (spot).

Папка на сервере: `/opt/footprint`. Мост Upscale живёт отдельно в `/root/bridge`. Единственная точка соприкосновения:
маршрут `/fp/` в Caddyfile моста (ставит `caddy_route.sh`), чтобы Grafana открывалась по HTTPS.

Команды (на сервере, из любой папки):

- `bash /opt/footprint/check.sh` — сверка всех 14 потоков с официальными данными бирж: объём по минутным свечам, покупки по свечам Binance и по ленте сделок Bybit, OKX, Coinbase, свечи (OHLC) против свечей Binance. Плюс пульс потоков.
- `bash /opt/footprint/logs.sh` — лог коллектора: раз в минуту строка `stats` со сделками в минуту по каждому потоку.
- `bash /opt/footprint/update.sh` — скачать свежий код из этого репозитория, проверить sha256 и перезапустить коллектор (и Grafana, если она запущена).
- `bash /opt/footprint/grafana.sh` — запустить Grafana с дашбордом footprint (создаёт read-only пользователя базы и пароль админа в `.env`).
- `bash /opt/footprint/caddy_route.sh` — открыть Grafana по адресу https://andrey-bridge.duckdns.org/fp/ (правит Caddyfile моста с резервной копией и автооткатом).
- `cd /opt/footprint && docker compose restart collector` — перезапуск сборщика.

Таблицы: `fp_1m` (минута x уровень x биржа x рынок), `ohlc_1m` (минутные свечи по каждому потоку),
`big_trades` (сделки от $100k), `heartbeat` (пульс).
Шаг уровня: BTC $10, ETH $1. Крупнее шаг и любой таймфрейм собираются запросом из минутных ячеек.

Дашборд: код панели — `grafana/footprint_panel.js` (Business Charts / ECharts), SQL — в `grafana/build_dashboard.py`.
После правки: `python3 grafana/build_dashboard.py` пересобирает `grafana/provisioning/dashboards/footprint.json`,
Grafana подхватывает файл сама. В браузере дашборд не редактируется — источник правды в репозитории.

Первая установка (в консоли сервера, команды без Shift):

```
mkdir -p /opt/footprint; cd /opt/footprint
curl -fsSL -o /tmp/fp.tgz github.com/1815071-ship-it/footprint/archive/refs/heads/main.tar.gz
tar xzf /tmp/fp.tgz --strip-components=1; sha256sum -c SHA256SUMS
bash setup.sh
```

Секретов в репозитории нет: пароли базы и Grafana генерируются на сервере в `.env` (файл в `.gitignore`).
