# Footprint

Живые кластеры Sell / Buy / Delta по BTC и ETH: Binance, Bybit, OKX (spot и perp), Coinbase (spot).

Папка на сервере: `/opt/footprint`. Мост Upscale живёт отдельно в `/root/bridge`, этот проект его не трогает.

Команды (на сервере, из любой папки):

- `bash /opt/footprint/check.sh` — сверка всех 14 потоков с официальными данными бирж: объём по минутным свечам, покупки по свечам Binance и по ленте сделок Bybit, OKX, Coinbase. Плюс пульс потоков.
- `bash /opt/footprint/logs.sh` — лог коллектора: раз в минуту строка `stats` со сделками в минуту по каждому потоку.
- `bash /opt/footprint/update.sh` — скачать свежий код из этого репозитория, проверить sha256 и перезапустить коллектор.
- `cd /opt/footprint && docker compose restart collector` — перезапуск сборщика.
- `cd /opt/footprint && docker compose --profile ui up -d` — включить Grafana (шаг 5).

Таблицы: `fp_1m` (минута x уровень x биржа x рынок), `big_trades` (сделки от $100k), `heartbeat` (пульс).
Шаг уровня: BTC $10, ETH $1. Крупнее шаг и любой таймфрейм собираются запросом из минутных ячеек.

Первая установка (в консоли сервера, команды без Shift):

```
mkdir -p /opt/footprint; cd /opt/footprint
curl -fsSL -o /tmp/fp.tgz github.com/1815071-ship-it/footprint/archive/refs/heads/main.tar.gz
tar xzf /tmp/fp.tgz --strip-components=1; sha256sum -c SHA256SUMS
bash setup.sh
```

Секретов в репозитории нет: пароль базы генерируется на сервере в `.env` (файл в `.gitignore`).
