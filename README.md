# Footprint

Живые кластеры Sell / Buy / Delta по BTC и ETH: Binance, Bybit, OKX (spot и perp), Coinbase (spot).
Плюс стакан: крупные лимитные заявки Binance, Bybit, OKX (spot и perp) по уровням цены, снимок раз в 15 секунд.

Папка на сервере: `/opt/footprint`. Мост Upscale живёт отдельно в `/root/bridge`. Единственная точка соприкосновения:
маршрут `/fp/` в Caddyfile моста (ставит `caddy_route.sh`), чтобы Grafana открывалась по HTTPS.

Команды (на сервере, из любой папки):

- `bash /opt/footprint/check.sh` — сверка всех 14 потоков с официальными данными бирж: объём по минутным свечам, покупки по свечам Binance и по ленте сделок Bybit, OKX, Coinbase, свечи (OHLC) против свечей Binance. Плюс пульс потоков. Если запущен сборщик стакана — ещё сверка стакана (последний снимок в базе против стакана биржи сейчас).
- `bash /opt/footprint/book.sh` — собрать и запустить сборщик стакана (сборщик сделок не перезапускается), показать память сервера и лог.
- `bash /opt/footprint/logs.sh` — лог коллектора: раз в минуту строка `stats`: сделки в минуту по каждому потоку, переподключения сторожа по копиям (`binance#0`, `binance#1`), `all copies silent` — сколько раз поток молчал на всех копиях сразу (это и есть потеря сделок), `dup%` — доля сделок, пришедших по обеим копиям (около 100% — обе копии живы).

К каждой бирже коллектор держит 2 независимые копии соединений (`REPLICAS` в docker-compose.yml): сделка попадает в базу с той копии, что принесла её первой, повтор отбрасывается по номеру сделки. Так зависание одного соединения не теряет сделки.
- `bash /opt/footprint/update.sh` — скачать свежий код из этого репозитория, проверить sha256 и перезапустить коллектор (и Grafana, если она запущена).
- `bash /opt/footprint/grafana.sh` — запустить Grafana с дашбордом footprint (создаёт read-only пользователя базы и пароль админа в `.env`).
- `bash /opt/footprint/caddy_route.sh` — открыть Grafana по адресу https://andrey-bridge.duckdns.org/fp/ (правит Caddyfile моста с резервной копией и автооткатом).
- `cd /opt/footprint && docker compose restart collector` — перезапуск сборщика.

Таблицы: `fp_1m` (минута x уровень x биржа x рынок), `ohlc_1m` (минутные свечи по каждому потоку),
`big_trades` (рыночные заявки от $100k: все исполнения с одним временем и стороной, цена средняя по $), `heartbeat` (пульс).
Стакан: `book_1m` (минута x уровень x сторона x биржа x рынок: средний, максимальный и последний объём заявок в $),
`book_meta_1m` (по каждому потоку и минуте: число снимков, лучшие цены, докуда видна глубина). Хранится 60 дней (около 250 МБ в сутки).
Стакан снимается через REST, а не вебсокет: вебсокет-стаканы Bybit и OKX покрывают у BTC лишь ±0.06–0.26% от цены,
REST — до ±1–3% (Binance spot 5000 уровней, OKX books-full 5000, Bybit perp 1000; Bybit spot отдаёт только 200).
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
