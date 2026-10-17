"""Footprint-коллектор: сделки с бирж -> минутные кластеры в Postgres.

Каждая сделка попадает в ячейку (минута, уровень цены, биржа, рынок, монета).
Раз в 2 секунды накопленные ячейки дописываются в fp_1m (объёмы складываются).

Двойное подключение: к каждой бирже открыто REPLICAS независимых копий соединений (по умолчанию 2).
Обе копии получают одни и те же сделки, в базу попадает первая пришедшая, повтор отбрасывается
по номеру сделки. 17.10 соединения с Binance, OKX и Bybit зависали по отдельности (Binance spot ETH
каждые 4-6 минут), поэтому пока одна копия молчит, сделки приходят по другой.
"""
import asyncio
import collections
import logging
import math
import os
import resource
import signal
import time
from datetime import datetime, timezone

import asyncpg
import ccxt.pro as ccxtpro
from ccxt.base.errors import NetworkError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fp")

STEP = {"BTC": 10.0, "ETH": 1.0}            # шаг уровня цены
BIG_USD = float(os.getenv("BIG_USD", "100000"))  # порог крупной сделки
FLUSH_SEC = 2.0
STATS_SEC = 60.0
# Сторож: если поток молчит дольше порога, закрываем соединения этой биржи - стримы переподключатся.
# Порог = max(STALL_SEC, 8 средних интервалов между сделками потока), но не больше 600 с,
# чтобы тихие потоки (OKX spot ETH ночью) не дёргали переподключение зря.
STALL_SEC = float(os.getenv("STALL_SEC", "90"))
WATCH_SEC = 5.0
REPLICAS = max(1, int(os.getenv("REPLICAS", "2")))  # сколько независимых копий соединений держать к каждой бирже
SEEN_MAX = 20000  # сколько последних номеров сделок помнить на поток для отбрасывания повторов

# (биржа, рынок, класс ccxt, {монета: символ ccxt})
VENUES = [
    ("binance", "spot", "binance", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"}),
    ("binance", "perp", "binanceusdm", {"BTC": "BTC/USDT:USDT", "ETH": "ETH/USDT:USDT"}),
    ("bybit", "spot", "bybit", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"}),
    ("bybit", "perp", "bybit", {"BTC": "BTC/USDT:USDT", "ETH": "ETH/USDT:USDT"}),
    ("okx", "spot", "okx", {"BTC": "BTC/USDT", "ETH": "ETH/USDT"}),
    ("okx", "perp", "okx", {"BTC": "BTC/USDT:USDT", "ETH": "ETH/USDT:USDT"}),
    ("coinbase", "spot", "coinbaseexchange", {"BTC": "BTC/USD", "ETH": "ETH/USD"}),
]

# Грузим только нужные типы рынков, чтобы не съесть память справочниками бирж.
EX_OPTIONS = {
    # Binance spot тоже на aggTrade (сделки одного ордера по одной цене склеены): в потоке trade рыночный ордер,
    # сметающий стакан, даёт сотни сообщений разом, кэш ccxt (1000 сделок) переполняется раньше, чем мы их заберём,
    # и часть сделок теряется. 17.10 так терялось ~12% объёма Binance spot ETH, в основном продажи.
    "binance": {"fetchMarkets": {"types": ["spot"]}, "watchTrades": {"name": "aggTrade"}},
    "binanceusdm": {"fetchMarkets": {"types": ["linear"]}, "watchTrades": {"name": "aggTrade"}},
    "bybit": {"fetchMarkets": {"types": ["spot", "linear"]}},
    "okx": {"fetchMarkets": {"types": ["spot", "swap"]}},
    "coinbaseexchange": {},
}

UPSERT_CELLS = """
INSERT INTO fp_1m (ts, exchange, market, symbol, price, buy_vol, sell_vol, buy_cnt, sell_cnt)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (symbol, ts, exchange, market, price) DO UPDATE SET
    buy_vol  = fp_1m.buy_vol  + EXCLUDED.buy_vol,
    sell_vol = fp_1m.sell_vol + EXCLUDED.sell_vol,
    buy_cnt  = fp_1m.buy_cnt  + EXCLUDED.buy_cnt,
    sell_cnt = fp_1m.sell_cnt + EXCLUDED.sell_cnt
"""
INSERT_BIG = """
INSERT INTO big_trades (ts, exchange, market, symbol, side, price, qty, usd, trade_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"""
UPSERT_HB = """
INSERT INTO heartbeat (exchange, market, symbol, last_trade, last_seen, trades, started)
VALUES ($1, $2, $3, $4, now(), $5, $6)
ON CONFLICT (exchange, market, symbol) DO UPDATE SET
    last_trade = EXCLUDED.last_trade, last_seen = now(),
    trades = EXCLUDED.trades, started = EXCLUDED.started
"""

# Минутные свечи по каждому потоку. Таблица создаётся здесь, а не в schema.sql:
# schema.sql выполняется только при самом первом запуске базы, а база уже работает.
OHLC_DDL = """
CREATE TABLE IF NOT EXISTS ohlc_1m (
    ts        timestamptz      NOT NULL,  -- начало минуты, UTC
    exchange  text             NOT NULL,
    market    text             NOT NULL,
    symbol    text             NOT NULL,
    open_ts   bigint           NOT NULL,  -- время первой сделки минуты, мс (чтобы склеивать записи)
    open      double precision NOT NULL,
    high      double precision NOT NULL,
    low       double precision NOT NULL,
    close_ts  bigint           NOT NULL,  -- время последней сделки минуты, мс
    close     double precision NOT NULL,
    PRIMARY KEY (symbol, ts, exchange, market)
)
"""
# В SET все ohlc_1m.* - это старые значения строки, поэтому порядок присваиваний не важен.
UPSERT_OHLC = """
INSERT INTO ohlc_1m (ts, exchange, market, symbol, open_ts, open, high, low, close_ts, close)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (symbol, ts, exchange, market) DO UPDATE SET
    open     = CASE WHEN EXCLUDED.open_ts < ohlc_1m.open_ts THEN EXCLUDED.open ELSE ohlc_1m.open END,
    open_ts  = LEAST(ohlc_1m.open_ts, EXCLUDED.open_ts),
    high     = GREATEST(ohlc_1m.high, EXCLUDED.high),
    low      = LEAST(ohlc_1m.low, EXCLUDED.low),
    close    = CASE WHEN EXCLUDED.close_ts >= ohlc_1m.close_ts THEN EXCLUDED.close ELSE ohlc_1m.close END,
    close_ts = GREATEST(ohlc_1m.close_ts, EXCLUDED.close_ts)
"""


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def new_cells():
    return collections.defaultdict(lambda: [0.0, 0.0, 0, 0])


def merge_ohlc(store, key, v):
    """Склеивает свечу v = [open_key, open, high, low, close_key, close] с уже накопленной.
    open_key/close_key = (время мс, номер сделки): при одинаковой миллисекунде порядок решает номер сделки,
    как у биржи (рыночный ордер, прошедший несколько уровней, даёт несколько сделок в одну мс)."""
    cur = store.get(key)
    if cur is None:
        store[key] = list(v)
        return
    if v[0] < cur[0]:
        cur[0], cur[1] = v[0], v[1]
    cur[2] = max(cur[2], v[2])
    cur[3] = min(cur[3], v[3])
    if v[4] >= cur[4]:
        cur[4], cur[5] = v[4], v[5]


def trade_order(ts, tid):
    """Ключ порядка сделки: время, затем номер сделки, если он числовой."""
    return (ts, int(tid) if tid and tid.isdigit() else 0)


class Agg:
    """Накопитель между записями в базу."""

    def __init__(self):
        self.cells = new_cells()
        self.ohlc = {}          # (минута, биржа, рынок, монета) -> [open_ts, open, high, low, close_ts, close]
        self.big = []
        self.hb = {}            # (биржа, рынок, монета) -> [последняя сделка ms, сделок всего]
        self.minute_trades = collections.Counter()
        self.started = datetime.now(timezone.utc)
        # (класс ccxt, копия, биржа, рынок, монета) -> monotonic-время последней пачки сделок от этой копии
        self.seen_at = {}
        self.resets = collections.Counter()  # (класс ccxt, копия) -> сколько раз сторож её переподключал
        self.blackouts = collections.Counter()  # (биржа, рынок, монета) -> сколько раз молчали все копии сразу
        self.dark = set()       # потоки, у которых сейчас молчат все копии (чтобы считать каждый случай один раз)
        self.stream_seen = {}   # (биржа, рынок, монета) -> monotonic-время последней пачки сделок по любой копии
        self.seen = {}          # (биржа, рынок, монета) -> (очередь, множество) последних номеров сделок
        self.dupes = collections.Counter()  # (биржа, рынок, монета) -> повторов за минуту (сделка пришла по обеим копиям)

    def first_time(self, stream_key, tid):
        """True, если сделку с этим номером видим впервые (по любой из копий соединения)."""
        q, ids = self.seen.setdefault(stream_key, (collections.deque(), set()))
        if tid in ids:
            self.dupes[stream_key] += 1
            return False
        q.append(tid)
        ids.add(tid)
        if len(q) > SEEN_MAX:
            ids.discard(q.popleft())
        return True

    def add(self, exchange, market, sym, ts, price, qty, side, tid):
        minute = ts // 60000 * 60000
        step = STEP[sym]
        level = math.floor(price / step) * step
        cell = self.cells[(minute, exchange, market, sym, level)]
        if side == "buy":
            cell[0] += qty
            cell[2] += 1
        else:
            cell[1] += qty
            cell[3] += 1
        order = trade_order(ts, tid)
        merge_ohlc(self.ohlc, (minute, exchange, market, sym), [order, price, price, price, order, price])
        usd = price * qty
        if usd >= BIG_USD:
            self.big.append((utc(ts), exchange, market, sym, side, price, qty, usd, tid))
        hb = self.hb.setdefault((exchange, market, sym), [0, 0])
        hb[0] = max(hb[0], ts)
        hb[1] += 1
        self.minute_trades[(exchange, market, sym)] += 1

    def merge_back(self, cells, big, ohlc):
        """Если запись в базу не удалась, возвращаем данные в накопитель."""
        for k, v in cells.items():
            c = self.cells[k]
            for i in range(4):
                c[i] += v[i]
        for k, v in ohlc.items():
            merge_ohlc(self.ohlc, k, v)
        self.big = big + self.big


async def ensure_markets(ex, lock, name):
    async with lock:
        delay = 5
        while not ex.markets:
            try:
                await ex.load_markets()
                log.info("%s: markets loaded (%d)", name, len(ex.markets))
            except Exception as e:  # noqa: BLE001
                log.warning("%s: load_markets failed: %s; retry in %ds", name, e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 300)


async def stream(ex, lock, cls, replica, exchange, market, sym, csym, agg):
    await ensure_markets(ex, lock, f"{cls}#{replica}")
    m = ex.market(csym)
    contract_size = float(m.get("contractSize") or 1) if m.get("contract") else 1.0
    log.info("start %s %s %s #%d (%s, contractSize=%s)", exchange, market, sym, replica, csym, contract_size)
    key = (cls, replica, exchange, market, sym)
    skey = (exchange, market, sym)
    agg.seen_at[key] = agg.stream_seen.setdefault(skey, time.monotonic())
    backoff = 1
    while True:
        try:
            trades = await ex.watch_trades(csym)
            backoff = 1
        except asyncio.CancelledError:
            # ccxt при закрытии соединения отменяет свои ожидания - это не остановка коллектора
            if asyncio.current_task().cancelling():
                raise
            log.warning("%s %s %s #%d: wait cancelled by ccxt; reconnect in %ds", exchange, market, sym, replica, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        except Exception as e:  # noqa: BLE001
            log.warning("%s %s %s #%d: %s; reconnect in %ds", exchange, market, sym, replica, str(e)[:200], backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        agg.seen_at[key] = agg.stream_seen[skey] = time.monotonic()
        for t in trades:
            tid = t.get("id")
            if tid is None:
                if replica != 0:
                    continue  # без номера повтор не распознать: такие сделки берём только из первой копии
            else:
                tid = str(tid)
                if not agg.first_time(skey, tid):
                    continue
            price, amount, ts, side = t.get("price"), t.get("amount"), t.get("timestamp"), t.get("side")
            if not price or not amount or ts is None or side not in ("buy", "sell"):
                continue
            agg.add(exchange, market, sym, int(ts), float(price), float(amount) * contract_size, side, tid)


async def flush(pool, agg):
    cells, agg.cells = agg.cells, new_cells()
    ohlc, agg.ohlc = agg.ohlc, {}
    big, agg.big = agg.big, []
    hb = [(e, m, s, utc(v[0]), v[1], agg.started) for (e, m, s), v in agg.hb.items()]
    if not cells and not big and not hb:
        return 0
    rows = [(utc(k[0]), k[1], k[2], k[3], k[4], v[0], v[1], v[2], v[3]) for k, v in cells.items()]
    candles = [(utc(k[0]), k[1], k[2], k[3], v[0][0], v[1], v[2], v[3], v[4][0], v[5]) for k, v in ohlc.items()]
    try:
        async with pool.acquire() as con:
            async with con.transaction():
                if rows:
                    await con.executemany(UPSERT_CELLS, rows)
                if candles:
                    await con.executemany(UPSERT_OHLC, candles)
                if big:
                    await con.executemany(INSERT_BIG, big)
                if hb:
                    await con.executemany(UPSERT_HB, hb)
    except Exception as e:  # noqa: BLE001
        log.error("db write failed: %s; keeping %d cells in memory", e, len(cells))
        agg.merge_back(cells, big, ohlc)
        return 0
    return len(rows)


async def flusher(pool, agg, stop):
    written, last_stats = 0, time.monotonic()
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=FLUSH_SEC)
        except asyncio.TimeoutError:
            pass
        written += await flush(pool, agg)
        if time.monotonic() - last_stats >= STATS_SEC:
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
            per = " ".join(f"{e}/{m}/{s}={n}" for (e, m, s), n in sorted(agg.minute_trades.items()))
            resets = " ".join(f"{c}#{r}={n}" for (c, r), n in sorted(agg.resets.items())) or "0"
            dark = " ".join(f"{e}/{m}/{s}={n}" for (e, m, s), n in sorted(agg.blackouts.items())) or "0"
            # доля сделок, пришедших повторно по второй копии: ~100% значит, что обе копии живы
            dup = " ".join(f"{e}/{m}/{s}={agg.dupes[(e, m, s)] * 100 // max(n, 1)}%"
                           for (e, m, s), n in sorted(agg.minute_trades.items()))
            log.info("stats: rows written %d, peak RSS %d MB, watchdog resets: %s, all copies silent: %s, "
                     "trades/min: %s, dup%%: %s", written, rss, resets, dark, per or "none", dup or "none")
            agg.minute_trades.clear()
            agg.dupes.clear()
            written, last_stats = 0, time.monotonic()


def stall_limit(agg, key, elapsed):
    """Сколько секунд тишины терпим для потока: 8 средних интервалов между сделками, от STALL_SEC до 600 с.
    key = (класс ccxt, копия, биржа, рынок, монета)."""
    total = agg.hb.get(key[2:], [0, 0])[1]
    avg = elapsed / total if total else STALL_SEC / 4
    return min(600.0, max(STALL_SEC, 8 * avg))


async def watchdog(exchanges, agg, stop):
    """Сторож потоков. Вебсокет биржи иногда замолкает без ошибки (так 17.10 встал Binance spot),
    и ожидание сделок висит вечно. Если поток молчит на какой-то копии соединения дольше порога,
    роняем соединения этой копии так же, как это делает сама ccxt при пропущенном ping-pong:
    client.on_error(...). Ожидающие стримы получают NetworkError и переподключаются по своему циклу.
    Другая копия при этом продолжает работать. Если молчат все копии потока сразу - это и есть
    потеря сделок, считаем такие случаи в agg.blackouts."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=WATCH_SEC)
        except asyncio.TimeoutError:
            pass
        now = time.monotonic()
        elapsed = (datetime.now(timezone.utc) - agg.started).total_seconds()
        silent_keys = {k for k, t in agg.seen_at.items() if now - t > stall_limit(agg, k, elapsed)}
        # поток целиком: ни одна копия не приносила сделок дольше порога - это и есть потеря данных
        for skey, t in agg.stream_seen.items():
            if now - t > stall_limit(agg, (None, None) + skey, elapsed):
                if skey not in agg.dark:
                    agg.dark.add(skey)
                    agg.blackouts[skey] += 1
                    log.warning("watchdog: %s %s %s - no trades on any of %d connection copies for %.0fs",
                                *skey, REPLICAS, now - t)
            else:
                agg.dark.discard(skey)
        for cls, copies in exchanges.items():
            for replica, ex in enumerate(copies):
                silent = [f"{k[2]} {k[3]} {k[4]} {now - agg.seen_at[k]:.0f}s" for k in silent_keys
                          if k[0] == cls and k[1] == replica]
                if not silent:
                    continue
                agg.resets[(cls, replica)] += 1
                clients = list(getattr(ex, "clients", {}).values())
                log.warning("watchdog: %s#%d silent (%s); force reconnect #%d, %d ws connection(s)",
                            cls, replica, ", ".join(silent), agg.resets[(cls, replica)], len(clients))
                for k in agg.seen_at:
                    if k[0] == cls and k[1] == replica:
                        agg.seen_at[k] = now  # даём время переподключиться
                for client in clients:
                    try:
                        client.on_error(NetworkError(f"watchdog: no trades ({', '.join(silent)})"))
                    except Exception as e:  # noqa: BLE001
                        log.warning("watchdog: %s#%d on_error failed: %s", cls, replica, str(e)[:200])


async def connect_db():
    dsn = os.environ["PG_DSN"]
    delay = 2
    while True:
        try:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            async with pool.acquire() as con:
                await con.execute(OHLC_DDL)
            return pool
        except Exception as e:  # noqa: BLE001
            log.warning("db not ready: %s; retry in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    pool = await connect_db()
    agg = Agg()
    exchanges, locks, tasks = {}, {}, []  # класс ccxt -> [копия 0, копия 1, ...]
    for exchange, market, cls, syms in VENUES:
        if cls not in exchanges:
            exchanges[cls] = [getattr(ccxtpro, cls)({"enableRateLimit": True, "options": EX_OPTIONS.get(cls, {})})
                              for _ in range(REPLICAS)]
            locks[cls] = [asyncio.Lock() for _ in range(REPLICAS)]
        for replica in range(REPLICAS):
            for sym, csym in syms.items():
                tasks.append(asyncio.create_task(stream(exchanges[cls][replica], locks[cls][replica], cls, replica,
                                                        exchange, market, sym, csym, agg)))
    tasks.append(asyncio.create_task(watchdog(exchanges, agg, stop)))
    log.info("collector started: %d streams x %d connection copies, flush every %ss, big trade >= $%d, "
             "stall limit >= %ds", (len(tasks) - 1) // REPLICAS, REPLICAS, FLUSH_SEC, BIG_USD, STALL_SEC)

    await flusher(pool, agg, stop)  # работает до SIGTERM
    log.info("stopping: final flush")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await flush(pool, agg)
    for copies in exchanges.values():
        for ex in copies:
            try:
                await ex.close()
            except Exception:  # noqa: BLE001
                pass
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
