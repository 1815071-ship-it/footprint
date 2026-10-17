"""Сверка стакана: последний снимок в базе против стакана, который биржа отдаёт прямо сейчас.

Это проверка конвейера (разбор ответа биржи, раскладка по уровням, запись в базу), а не независимый
источник: снимки в базе сделаны тем же запросом. Между снимком в базе и свежим проходит до BOOK_SEC
секунд, стакан у цены за это время меняется, поэтому сравнение грубое:
  объём:   сумма заявок от порога ($50k BTC, $20k ETH) в общей зоне видимости, наш против биржи;
  крупные: сколько из 10 крупнейших уровней биржи есть среди 20 крупнейших у нас.
Плюс регулярность: сколько снимков за последние 5 полных минут, и сколько места занимают таблицы.
"""
import asyncio
import os
import time
from datetime import datetime, timezone

import asyncpg

from book import BOOK_SEC, LIMIT, MIN_USD, STREAMS, bucketize, http_json, okx_contract_sizes, parse, url_for, visible, STEP

TOL_PCT = 15.0      # допуск по объёму: стакан у цены живой, за 15 секунд меняется
WALLS_MIN = 7       # из 10 крупнейших уровней биржи у нас должно найтись не меньше


def top(levels, n):
    return {k for k, _ in sorted(levels.items(), key=lambda kv: -kv[1])[:n]}


async def check_stream(con, key):
    exchange, market, sym = key
    name = f"{exchange} {market} {sym}"
    meta = await con.fetchrow(
        "SELECT * FROM book_meta_1m WHERE exchange = $1 AND market = $2 AND symbol = $3 ORDER BY ts DESC LIMIT 1",
        exchange, market, sym)
    if meta is None:
        return name, None, "снимков в базе нет"
    now = time.time()
    cur_min = int(now // 60 * 60)
    snaps5 = await con.fetchval(
        "SELECT coalesce(sum(snaps), 0) FROM book_meta_1m WHERE exchange = $1 AND market = $2 AND symbol = $3 "
        "AND ts >= to_timestamp($4) AND ts < to_timestamp($5)", exchange, market, sym, cur_min - 300, cur_min)
    expect = int(300 / BOOK_SEC)
    rows = await con.fetch(
        "SELECT side, price, last_usd FROM book_1m WHERE exchange = $1 AND market = $2 AND symbol = $3 AND ts = $4",
        exchange, market, sym, meta["ts"])
    try:
        j, _ = await asyncio.to_thread(http_json, url_for(*key))
        bids, asks = parse(exchange, market, sym, j)
    except Exception as e:  # noqa: BLE001
        return name, None, f"биржа не ответила: {str(e)[:80]}"
    fetched = time.time()
    fresh = bucketize(bids, asks, sym, LIMIT[(exchange, market)])
    if fresh is None:
        return name, None, "биржа отдала пустой стакан"
    step = STEP[sym]
    floor = max(meta["bid_floor"], fresh.floor)
    ceil = min(meta["ask_ceil"], fresh.ceil)
    thr = MIN_USD[sym]
    ours = {(r["side"], r["price"]): r["last_usd"] for r in rows
            if r["last_usd"] >= thr and visible(r["side"], r["price"], step, floor, ceil)}
    theirs = {k: v for k, v in fresh.buckets.items() if v >= thr and visible(k[0], k[1], step, floor, ceil)}
    o, t = sum(ours.values()), sum(theirs.values())
    diff = (o - t) / t * 100 if t else (0.0 if o == 0 else 100.0)
    walls = len(top(theirs, 10) & top(ours, 20))
    nwalls = min(10, len(theirs))
    mid = (fresh.best_bid + fresh.best_ask) / 2
    lag = fetched - meta["last_snap"].timestamp()
    problems = []
    if snaps5 < 0.8 * expect:
        problems.append("мало снимков")
    if lag > 3 * BOOK_SEC:
        problems.append("снимки отстают")
    if abs(diff) > TOL_PCT:
        problems.append("объём")
    if walls < min(WALLS_MIN, nwalls):
        problems.append("крупные уровни")
    res = {
        "snaps": f"{snaps5}/{expect}",
        "depth": f"-{(1 - meta['bid_floor'] / mid) * 100:.2f}% +{(meta['ask_ceil'] / mid - 1) * 100:.2f}%",
        "lag": f"{lag:.0f}с",
        "ours": o / 1e6, "theirs": t / 1e6, "diff": diff,
        "walls": f"{walls}/{nwalls}",
        "verdict": "OK" if not problems else "ПРОВЕРИТЬ: " + ", ".join(problems),
    }
    return name, res, None


async def main():
    con = await asyncpg.connect(os.environ["PG_DSN"])
    exists = await con.fetchval("SELECT to_regclass('book_meta_1m') IS NOT NULL")
    if not exists:
        print("стакан: таблиц ещё нет, сборщик стакана не запускался")
        await con.close()
        return
    await okx_contract_sizes()
    print(f"\nстакан: последний снимок в базе против стакана биржи сейчас (уровни от ${MIN_USD['BTC'] / 1e3:.0f}k BTC, "
          f"${MIN_USD['ETH'] / 1e3:.0f}k ETH; допуск по объёму {TOL_PCT:g}%)")
    print(f"{'поток':<18}{'снимков 5м':>11}{'глубина':>16}{'отст.':>6}{'наш $M':>9}{'биржа $M':>9}{'разн.%':>8}"
          f"{'крупные':>9}  итог")
    bad = []
    for key in STREAMS:
        name, r, err = await check_stream(con, key)
        if err:
            print(f"{name:<18} {err}")
            bad.append(name)
            continue
        print(f"{name:<18}{r['snaps']:>11}{r['depth']:>16}{r['lag']:>6}{r['ours']:>9.2f}{r['theirs']:>9.2f}"
              f"{r['diff']:>8.1f}{r['walls']:>9}  {r['verdict']}")
        if r["verdict"] != "OK":
            bad.append(name)
    # Прогноз на сутки: строки за последний час (или с первой записи, если сборщик работает меньше часа)
    # делим на фактически покрытые минуты. Текущую неполную минуту не считаем.
    hour, first, last = await con.fetchrow(
        "SELECT count(*), min(ts), max(ts) FROM book_1m "
        "WHERE ts >= date_trunc('minute', now()) - interval '60 minutes' AND ts < date_trunc('minute', now())")
    total = await con.fetchval("SELECT count(*) FROM book_1m")
    size = await con.fetchval("SELECT pg_total_relation_size('book_1m') + pg_total_relation_size('book_meta_1m')")
    per_row = size / total if total else 0
    mins = (last - first).total_seconds() / 60 + 1 if first else 0
    per_day = hour / mins * 1440 if mins else 0
    print(f"\nbook_1m: {total} строк, {size / 2**20:.0f} МБ вместе с book_meta_1m. "
          f"За последние {mins:.0f} полных минут {hour} строк: в сутки ~{per_day / 1e6:.1f} млн строк "
          f"~{per_day * per_row / 2**20:.0f} МБ. Время сервера: {datetime.now(timezone.utc):%H:%M:%S} UTC")
    print("ИТОГ по стакану: " + ("всё сходится" if not bad else "проверить: " + ", ".join(bad)))
    await con.close()


if __name__ == "__main__":
    asyncio.run(main())
