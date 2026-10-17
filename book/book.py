"""Сборщик стакана: снимки книги заявок Binance, Bybit, OKX (spot и perp, BTC и ETH) -> Postgres.

Почему REST, а не вебсокет. Вебсокет-стаканы Bybit и OKX неглубокие: 200-1000 уровней покрывают
у BTC всего ±0.06-0.26% от цены (замер 17.10.2026). Крупные заявки, ради которых всё затевается, стоят дальше.
REST отдаёт глубже: Binance spot до 5000 уровней, OKX books-full 5000 (BTC ±1%, ETH ±2-3%), Bybit perp 1000.
Цена решения: снимок раз в BOOK_SEC секунд (15), заявку, которая прожила меньше, можем не увидеть.

Снимок раскладывается по уровням цены (BTC $10, ETH $1, как в fp_1m) в пределах ±BOOK_RANGE_PCT от середины.
Уровни копятся по минутам: средний, максимальный и последний объём заявок на уровне, в долларах.
Уровень, который снимок покрывал, но заявок на нём нет, считается нулём (заявки сняли или их съели).
После каждого снимка в базу пишется состояние текущей минуты целиком (перезапись, а не добавка),
поэтому неудачную запись исправляет следующий снимок.
"""
import asyncio
import collections
import gzip
import json
import logging
import math
import os
import resource
import signal
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("book")

STEP = {"BTC": 10.0, "ETH": 1.0}                 # шаг уровня цены, как в fp_1m
BOOK_SEC = float(os.getenv("BOOK_SEC", "15"))    # как часто снимаем каждый стакан
RANGE_PCT = float(os.getenv("BOOK_RANGE_PCT", "3"))
# В базу пишем уровни, где за минуту хоть раз стояло заявок не меньше порога: мелочь не нужна и раздувает таблицу.
MIN_USD = {"BTC": float(os.getenv("BOOK_MIN_USD_BTC", "50000")),
           "ETH": float(os.getenv("BOOK_MIN_USD_ETH", "20000"))}
KEEP_DAYS = int(os.getenv("BOOK_KEEP_DAYS", "7"))  # сколько дней хранить, старое удаляется раз в час
GAP = {"binance": 0.3, "bybit": 0.3, "okx": 1.1}   # пауза между запросами к одной бирже, секунд
# Binance считает «вес» запросов: 6000 в минуту на IP, стакан на 5000 уровней весит 250.
# Если вес за минуту уже выше порога, тяжёлый запрос пропускаем, чтобы не получить бан IP
# (бан задел бы и сборщик сделок на этом же сервере).
BINANCE_WEIGHT_MAX = 3600
HTTP_TIMEOUT = 10
STATS_SEC = 60.0
CLEAN_SEC = 3600.0
OKX_CT_VAL = {"BTC": 0.01, "ETH": 0.1}  # размер контракта OKX swap в монетах; на старте уточняем у биржи

STREAMS = [(e, m, s) for e in ("binance", "bybit", "okx") for m in ("spot", "perp") for s in ("BTC", "ETH")]

DDL = """
CREATE TABLE IF NOT EXISTS book_1m (
    ts        timestamptz      NOT NULL,  -- начало минуты, UTC
    exchange  text             NOT NULL,  -- binance | bybit | okx
    market    text             NOT NULL,  -- spot | perp
    symbol    text             NOT NULL,  -- BTC | ETH
    side      text             NOT NULL,  -- bid = заявки на покупку (ниже цены), ask = на продажу (выше)
    price     double precision NOT NULL,  -- нижняя граница уровня: BTC шаг $10, ETH шаг $1
    avg_usd   real             NOT NULL,  -- средний объём заявок на уровне за минуту, $
    max_usd   real             NOT NULL,  -- максимальный объём за минуту, $
    last_usd  real             NOT NULL,  -- объём в последнем снимке минуты, $; 0 = заявки исчезли
    snaps     smallint         NOT NULL,  -- в скольких снимках минуты уровень был виден
    PRIMARY KEY (symbol, ts, exchange, market, side, price)
);
CREATE TABLE IF NOT EXISTS book_meta_1m (
    ts        timestamptz      NOT NULL,  -- начало минуты, UTC
    exchange  text             NOT NULL,
    market    text             NOT NULL,
    symbol    text             NOT NULL,
    snaps     smallint         NOT NULL,  -- снимков за минуту
    last_snap timestamptz      NOT NULL,  -- время последнего снимка
    best_bid  double precision NOT NULL,  -- лучшие цены в последнем снимке
    best_ask  double precision NOT NULL,
    bid_floor double precision NOT NULL,  -- ниже этой цены заявки покупателей не видны (конец глубины или ±RANGE)
    ask_ceil  double precision NOT NULL,  -- выше этой цены не видны заявки продавцов
    PRIMARY KEY (symbol, ts, exchange, market)
);
"""
# В SET только новые значения: строка всегда несёт полное состояние минуты.
UPSERT_LEVELS = """
INSERT INTO book_1m (ts, exchange, market, symbol, side, price, avg_usd, max_usd, last_usd, snaps)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (symbol, ts, exchange, market, side, price) DO UPDATE SET
    avg_usd = EXCLUDED.avg_usd, max_usd = EXCLUDED.max_usd,
    last_usd = EXCLUDED.last_usd, snaps = EXCLUDED.snaps
"""
DELETE_MINUTE = "DELETE FROM book_1m WHERE exchange = $1 AND market = $2 AND symbol = $3 AND ts = $4"
UPSERT_META = """
INSERT INTO book_meta_1m (ts, exchange, market, symbol, snaps, last_snap, best_bid, best_ask, bid_floor, ask_ceil)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (symbol, ts, exchange, market) DO UPDATE SET
    snaps = EXCLUDED.snaps, last_snap = EXCLUDED.last_snap, best_bid = EXCLUDED.best_bid,
    best_ask = EXCLUDED.best_ask, bid_floor = EXCLUDED.bid_floor, ask_ceil = EXCLUDED.ask_ceil
"""


# ---------- биржи ----------

class HttpError(Exception):
    def __init__(self, status, retry_after, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status, self.retry_after = status, retry_after


def http_json(url):
    """GET -> (json, заголовки в нижнем регистре). Блокирующий, вызывается в потоке."""
    req = urllib.request.Request(url, headers={"User-Agent": "footprint-book/1.0", "Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            raw = r.read()
            headers = {k.lower(): v for k, v in r.headers.items()}
    except urllib.error.HTTPError as e:
        ra = e.headers.get("Retry-After") if e.headers else None
        raise HttpError(e.code, float(ra) if ra and ra.isdigit() else None, str(e.reason)[:100]) from None
    if headers.get("content-encoding") == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw), headers


def levels(rows, mult=1.0):
    """[[цена, количество, ...], ...] (строки) -> [(цена, количество в монетах)]."""
    return [(float(r[0]), float(r[1]) * mult) for r in rows]


# limit - сколько уровней на сторону просим; если пришло столько же, сторона обрезана и глубже не видно.
LIMIT = {
    ("binance", "spot"): 5000, ("binance", "perp"): 1000,
    ("bybit", "spot"): 200, ("bybit", "perp"): 1000,
    ("okx", "spot"): 5000, ("okx", "perp"): 5000,
}


def url_for(exchange, market, sym):
    lim = LIMIT[(exchange, market)]
    if exchange == "binance":
        if market == "spot":
            return f"https://api.binance.com/api/v3/depth?symbol={sym}USDT&limit={lim}"
        return f"https://fapi.binance.com/fapi/v1/depth?symbol={sym}USDT&limit={lim}"
    if exchange == "bybit":
        cat = "spot" if market == "spot" else "linear"
        return f"https://api.bybit.com/v5/market/orderbook?category={cat}&symbol={sym}USDT&limit={lim}"
    inst = f"{sym}-USDT" if market == "spot" else f"{sym}-USDT-SWAP"
    return f"https://www.okx.com/api/v5/market/books-full?instId={inst}&sz={lim}"


def parse(exchange, market, sym, j):
    """Ответ биржи -> (заявки на покупку, на продажу) в монетах."""
    if exchange == "binance":
        if "bids" not in j:
            raise RuntimeError(f"binance: {str(j)[:150]}")
        return levels(j["bids"]), levels(j["asks"])
    if exchange == "bybit":
        if j.get("retCode") != 0:
            raise RuntimeError(f"bybit retCode {j.get('retCode')}: {j.get('retMsg')}")
        return levels(j["result"]["b"]), levels(j["result"]["a"])
    if j.get("code") != "0" or not j.get("data"):
        raise RuntimeError(f"okx code {j.get('code')}: {j.get('msg')}")
    mult = OKX_CT_VAL[sym] if market == "perp" else 1.0  # у swap количество в контрактах
    d = j["data"][0]
    return levels(d["bids"], mult), levels(d["asks"], mult)


# ---------- уровни ----------

class Snap:
    __slots__ = ("best_bid", "best_ask", "floor", "ceil", "buckets")

    def __init__(self, best_bid, best_ask, floor, ceil, buckets):
        self.best_bid, self.best_ask, self.floor, self.ceil, self.buckets = best_bid, best_ask, floor, ceil, buckets


def visible(side, price, step, floor, ceil):
    """Покрывал ли снимок уровень [price, price+step) целиком."""
    if side == "bid":
        return price >= floor - 1e-9
    return price + step <= ceil + 1e-9


def bucketize(bids, asks, sym, limit, range_pct=None):
    """Снимок стакана -> Snap: объём заявок в $ по уровням цены в пределах ±range_pct от середины.
    Уровень на краю видимости (глубина кончилась посреди уровня) отбрасываем: он был бы занижен."""
    if not bids or not asks:
        return None
    rp = RANGE_PCT if range_pct is None else range_pct
    step = STEP[sym]
    best_bid = max(p for p, _ in bids)
    best_ask = min(p for p, _ in asks)
    if best_bid >= best_ask:
        return None  # перекрещенный стакан: битый ответ
    mid = (best_bid + best_ask) / 2
    floor = mid * (1 - rp / 100)
    ceil = mid * (1 + rp / 100)
    if len(bids) >= limit:
        floor = max(floor, min(p for p, _ in bids))
    if len(asks) >= limit:
        ceil = min(ceil, max(p for p, _ in asks))
    out = {}
    for side, rows in (("bid", bids), ("ask", asks)):
        for p, q in rows:
            if q <= 0:
                continue
            b = math.floor(p / step) * step
            if not visible(side, b, step, floor, ceil):
                continue
            k = (side, b)
            out[k] = out.get(k, 0.0) + p * q
    return Snap(best_bid, best_ask, floor, ceil, out)


class Minute:
    """Уровни одного потока за одну минуту: (сторона, цена) -> [сумма $, снимков, максимум $, последний $]."""

    def __init__(self, minute, step):
        self.minute, self.step = minute, step
        self.cells = {}
        self.cover = []        # (floor, ceil) каждого снимка минуты
        self.last = None       # последний Snap
        self.last_at = 0.0
        self.dirty = False     # есть изменения, ещё не записанные в базу
        self.written = False   # минута уже хоть раз записана этим процессом

    def add(self, snap, at):
        step = self.step
        for k, c in self.cells.items():
            if k not in snap.buckets and visible(k[0], k[1], step, snap.floor, snap.ceil):
                c[1] += 1      # уровень был виден, а заявок на нём нет: ноль
                c[3] = 0.0
        for k, usd in snap.buckets.items():
            c = self.cells.get(k)
            if c is None:
                # до этого снимка уровень мог быть виден пустым: те снимки тоже считаем (с нулём)
                seen = sum(1 for f, cl in self.cover if visible(k[0], k[1], step, f, cl))
                self.cells[k] = [usd, seen + 1, usd, usd]
            else:
                c[0] += usd
                c[1] += 1
                c[2] = max(c[2], usd)
                c[3] = usd
        self.cover.append((snap.floor, snap.ceil))
        self.last, self.last_at, self.dirty = snap, at, True

    def rows(self, exchange, market, sym, min_usd):
        ts = datetime.fromtimestamp(self.minute, tz=timezone.utc)
        return [(ts, exchange, market, sym, side, price, c[0] / c[1], c[2], c[3], c[1])
                for (side, price), c in self.cells.items() if c[2] >= min_usd]

    def meta(self, exchange, market, sym):
        s = self.last
        return (datetime.fromtimestamp(self.minute, tz=timezone.utc), exchange, market, sym, len(self.cover),
                datetime.fromtimestamp(self.last_at, tz=timezone.utc), s.best_bid, s.best_ask, s.floor, s.ceil)


# ---------- сбор ----------

class State:
    def __init__(self):
        self.minutes = {}                              # поток -> текущая Minute
        self.unsaved = collections.defaultdict(list)   # поток -> прошлые Minute, которые не удалось записать
        self.ok = collections.Counter()                # снимков за период статистики
        self.fail = collections.Counter()
        self.skipped = collections.Counter()
        self.rows = collections.Counter()
        self.last_err = {}
        self.blocked_until = {}                        # биржа -> monotonic: пауза после 429/418
        self.binance_weight = (0, 0.0)                 # (вес за минуту, monotonic когда узнали)


async def save(pool, st, key, aggs):
    exchange, market, sym = key
    rows, metas = [], []
    for a in aggs:
        rows += a.rows(exchange, market, sym, MIN_USD[sym])
        metas.append(a.meta(exchange, market, sym))
    try:
        async with pool.acquire() as con:
            async with con.transaction():
                for a in aggs:
                    if not a.written:
                        # строки этой минуты от прошлого запуска сборщика (перезапуск посреди минуты)
                        # перезаписали бы не все уровни: удаляем, минута пишется заново целиком
                        await con.execute(DELETE_MINUTE, exchange, market, sym,
                                          datetime.fromtimestamp(a.minute, tz=timezone.utc))
                if rows:
                    await con.executemany(UPSERT_LEVELS, rows)
                await con.executemany(UPSERT_META, metas)
    except Exception as e:  # noqa: BLE001
        log.error("%s %s %s: db write failed: %s", *key, str(e)[:200])
        return False
    for a in aggs:
        a.dirty = False
        a.written = True
    st.rows[key] += len(rows)
    return True


async def snap_one(pool, st, key):
    exchange, market, sym = key
    if exchange == "binance" and market == "spot":
        w, at = st.binance_weight
        if w > BINANCE_WEIGHT_MAX and time.monotonic() - at < 60:
            st.skipped[key] += 1
            return
    try:
        j, headers = await asyncio.wait_for(asyncio.to_thread(http_json, url_for(*key)), HTTP_TIMEOUT + 5)
        bids, asks = parse(exchange, market, sym, j)
    except HttpError as e:
        if e.status in (418, 429):
            pause = e.retry_after or (600 if e.status == 418 else 60)
            st.blocked_until[exchange] = time.monotonic() + pause
            log.error("%s: HTTP %d (лимит запросов), пауза %ds", exchange, e.status, pause)
        st.fail[key] += 1
        st.last_err[key] = str(e)[:120]
        return
    except Exception as e:  # noqa: BLE001
        msg = str(e)[:120] or type(e).__name__
        if exchange == "bybit" and "retCode" in msg and LIMIT[(exchange, market)] > 500:
            LIMIT[(exchange, market)] = 500  # если Bybit перестанет отдавать 1000 уровней
            log.warning("bybit %s: %s; дальше прошу 500 уровней", market, msg)
        st.fail[key] += 1
        st.last_err[key] = msg
        return
    if exchange == "binance" and headers.get("x-mbx-used-weight-1m", "").isdigit():
        st.binance_weight = (int(headers["x-mbx-used-weight-1m"]), time.monotonic())
    now = time.time()
    snap = bucketize(bids, asks, sym, LIMIT[(exchange, market)])
    if snap is None:
        st.fail[key] += 1
        st.last_err[key] = "пустой или перекрещенный стакан"
        return
    st.ok[key] += 1
    minute = int(now // 60 * 60)
    cur = st.minutes.get(key)
    if cur is None or cur.minute != minute:
        if cur is not None and cur.dirty:
            st.unsaved[key] = (st.unsaved[key] + [cur])[-5:]  # дольше 5 минут без базы - старое теряем
        cur = Minute(minute, STEP[sym])
        st.minutes[key] = cur
    cur.add(snap, now)
    pending = st.unsaved[key]
    if await save(pool, st, key, pending + [cur]):
        st.unsaved[key] = []


async def sleep_or_stop(stop, sec):
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(sec, 0.0))
    except asyncio.TimeoutError:
        pass


async def poll_exchange(pool, st, exchange, stop):
    """Все потоки одной биржи по очереди, с паузой GAP между запросами, круг раз в BOOK_SEC."""
    keys = [k for k in STREAMS if k[0] == exchange]
    while not stop.is_set():
        t0 = time.monotonic()
        for key in keys:
            if stop.is_set() or st.blocked_until.get(exchange, 0) > time.monotonic():
                break
            try:
                await snap_one(pool, st, key)
            except Exception as e:  # noqa: BLE001  страховка: поток не должен умирать
                st.fail[key] += 1
                st.last_err[key] = f"{type(e).__name__}: {str(e)[:100]}"
                log.exception("%s %s %s: unexpected error", *key)
            await sleep_or_stop(stop, GAP[exchange])
        wait = BOOK_SEC - (time.monotonic() - t0)
        blocked = st.blocked_until.get(exchange, 0) - time.monotonic()
        await sleep_or_stop(stop, max(wait, blocked, 0.5))


async def okx_contract_sizes():
    for sym in OKX_CT_VAL:
        url = f"https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId={sym}-USDT-SWAP"
        for attempt in range(3):
            try:
                j, _ = await asyncio.to_thread(http_json, url)
                OKX_CT_VAL[sym] = float(j["data"][0]["ctVal"])
                break
            except Exception as e:  # noqa: BLE001
                log.warning("okx ctVal %s: %s (attempt %d)", sym, str(e)[:120], attempt + 1)
                await asyncio.sleep(2)
        else:
            log.warning("okx ctVal %s: беру значение по умолчанию %s", sym, OKX_CT_VAL[sym])
    log.info("okx contract sizes: %s", OKX_CT_VAL)


async def stats(st, stop):
    while not stop.is_set():
        await sleep_or_stop(stop, STATS_SEC)
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
        parts = []
        for key in STREAMS:
            cur = st.minutes.get(key)
            cov = ""
            if cur is not None and cur.last is not None:
                mid = (cur.last.best_bid + cur.last.best_ask) / 2
                cov = f" -{(1 - cur.last.floor / mid) * 100:.2f}%+{(cur.last.ceil / mid - 1) * 100:.2f}%"
            bad = f" fail={st.fail[key]}" if st.fail[key] else ""
            skip = f" skip={st.skipped[key]}" if st.skipped[key] else ""
            parts.append(f"{key[0]}/{key[1]}/{key[2]}={st.ok[key]}{bad}{skip}{cov}")
        log.info("stats: snaps/min %s; rows written %d; binance weight %d; peak RSS %d MB",
                 " ".join(parts), sum(st.rows.values()), st.binance_weight[0], rss)
        errs = {k: v for k, v in st.last_err.items() if st.fail[k]}
        if errs:
            log.warning("errors: %s", "; ".join(f"{k[0]} {k[1]} {k[2]}: {v}" for k, v in errs.items()))
        st.ok.clear()
        st.fail.clear()
        st.skipped.clear()
        st.rows.clear()


async def cleaner(pool, stop):
    """Раз в час удаляем снимки старше BOOK_KEEP_DAYS, чтобы не забить диск."""
    while not stop.is_set():
        try:
            async with pool.acquire() as con:
                a = await con.execute("DELETE FROM book_1m WHERE ts < now() - make_interval(days => $1)", KEEP_DAYS)
                b = await con.execute("DELETE FROM book_meta_1m WHERE ts < now() - make_interval(days => $1)", KEEP_DAYS)
            log.info("cleanup older than %d days: book_1m %s, book_meta_1m %s", KEEP_DAYS, a, b)
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup failed: %s", str(e)[:200])
        await sleep_or_stop(stop, CLEAN_SEC)


async def connect_db():
    dsn = os.environ["PG_DSN"]
    delay = 2
    while True:
        try:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
            async with pool.acquire() as con:
                await con.execute(DDL)
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
    await okx_contract_sizes()
    st = State()
    log.info("book collector started: %d streams, snapshot every %ss, range ±%s%%, keep %d days, "
             "min level $%d BTC / $%d ETH", len(STREAMS), BOOK_SEC, RANGE_PCT, KEEP_DAYS, MIN_USD["BTC"], MIN_USD["ETH"])
    tasks = [asyncio.create_task(poll_exchange(pool, st, ex, stop)) for ex in ("binance", "bybit", "okx")]
    tasks += [asyncio.create_task(stats(st, stop)), asyncio.create_task(cleaner(pool, stop))]
    await stop.wait()
    log.info("stopping")
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
