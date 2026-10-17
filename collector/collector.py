"""Footprint-коллектор: сделки с бирж -> минутные кластеры в Postgres.

Каждая сделка попадает в ячейку (минута, уровень цены, биржа, рынок, монета).
Раз в 2 секунды накопленные ячейки дописываются в fp_1m (объёмы складываются).
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fp")

STEP = {"BTC": 10.0, "ETH": 1.0}            # шаг уровня цены
BIG_USD = float(os.getenv("BIG_USD", "100000"))  # порог крупной сделки
FLUSH_SEC = 2.0
STATS_SEC = 60.0

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
    "binance": {"fetchMarkets": {"types": ["spot"]}},
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
    """Склеивает свечу v = [open_ts, open, high, low, close_ts, close] с уже накопленной."""
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


class Agg:
    """Накопитель между записями в базу."""

    def __init__(self):
        self.cells = new_cells()
        self.ohlc = {}          # (минута, биржа, рынок, монета) -> [open_ts, open, high, low, close_ts, close]
        self.big = []
        self.hb = {}            # (биржа, рынок, монета) -> [последняя сделка ms, сделок всего]
        self.minute_trades = collections.Counter()
        self.started = datetime.now(timezone.utc)

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
        merge_ohlc(self.ohlc, (minute, exchange, market, sym), [ts, price, price, price, ts, price])
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


async def stream(ex, lock, cls, exchange, market, sym, csym, agg):
    await ensure_markets(ex, lock, cls)
    m = ex.market(csym)
    contract_size = float(m.get("contractSize") or 1) if m.get("contract") else 1.0
    log.info("start %s %s %s (%s, contractSize=%s)", exchange, market, sym, csym, contract_size)
    seen, seen_set = collections.deque(), set()
    backoff = 1
    while True:
        try:
            trades = await ex.watch_trades(csym)
            backoff = 1
        except Exception as e:  # noqa: BLE001
            log.warning("%s %s %s: %s; reconnect in %ds", exchange, market, sym, str(e)[:200], backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        for t in trades:
            tid = t.get("id")
            if tid is not None:
                tid = str(tid)
                if tid in seen_set:
                    continue
                seen.append(tid)
                seen_set.add(tid)
                if len(seen) > 20000:
                    seen_set.discard(seen.popleft())
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
    candles = [(utc(k[0]), k[1], k[2], k[3], *v) for k, v in ohlc.items()]
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
            log.info("stats: rows written %d, peak RSS %d MB, trades/min: %s", written, rss, per or "none")
            agg.minute_trades.clear()
            written, last_stats = 0, time.monotonic()


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
    exchanges, locks, tasks = {}, {}, []
    for exchange, market, cls, syms in VENUES:
        if cls not in exchanges:
            exchanges[cls] = getattr(ccxtpro, cls)({"enableRateLimit": True, "options": EX_OPTIONS.get(cls, {})})
            locks[cls] = asyncio.Lock()
        for sym, csym in syms.items():
            tasks.append(asyncio.create_task(stream(exchanges[cls], locks[cls], cls, exchange, market, sym, csym, agg)))
    log.info("collector started: %d streams, flush every %ss, big trade >= $%d", len(tasks), FLUSH_SEC, BIG_USD)

    await flusher(pool, agg, stop)  # работает до SIGTERM
    log.info("stopping: final flush")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await flush(pool, agg)
    for ex in exchanges.values():
        try:
            await ex.close()
        except Exception:  # noqa: BLE001
            pass
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
