"""Сверка с независимым источником: наши кластеры Binance против официальных 1m-свечей Binance.

Для каждой полной минуты сравниваем:
  объём:        sum(buy_vol + sell_vol)  vs  volume свечи
  покупки:      sum(buy_vol)             vs  taker buy base volume свечи
Расхождение больше 1% = ошибка в коллекторе.
"""
import asyncio
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import asyncpg

KLINES = {
    "spot": "https://api.binance.com/api/v3/klines?symbol={s}USDT&interval=1m&limit=60",
    "perp": "https://fapi.binance.com/fapi/v1/klines?symbol={s}USDT&interval=1m&limit=60",
}

# Наши минуты по одному потоку Binance: объём покупок и продаж на всех уровнях цены.
SQL = """
SELECT ts, sum(buy_vol) AS buy, sum(sell_vol) AS sell, sum(buy_cnt + sell_cnt) AS cnt
FROM fp_1m
WHERE exchange = 'binance' AND market = $1 AND symbol = $2
GROUP BY ts
ORDER BY ts
"""


def pct(a, b):
    return 0.0 if b == 0 else (a - b) / b * 100


async def main():
    con = await asyncpg.connect(os.environ["PG_DSN"])
    now = time.time()
    last_full = int((now - 70) // 60 * 60)  # последняя минута, закрытая хотя бы 10 с назад
    ok_all = True
    print(f"{'поток':<14}{'минут':>6}{'наш объём':>14}{'Binance':>14}{'разн.%':>8}{'buy разн.%':>12}  итог")
    for market, url in KLINES.items():
        for sym in ("BTC", "ETH"):
            ours = {int(r["ts"].timestamp()): r for r in await con.fetch(SQL, market, sym)}
            if not ours:
                print(f"{market + ' ' + sym:<14} нет данных")
                ok_all = False
                continue
            first = min(ours)  # первая минута неполная: коллектор стартовал посреди неё
            with urllib.request.urlopen(url.format(s=sym), timeout=15) as resp:
                klines = json.load(resp)
            n = our_v = ref_v = our_b = ref_b = 0.0
            for k in klines:
                t = int(k[0]) // 1000
                if t <= first or t > last_full:
                    continue
                r = ours.get(t)
                our_v += (r["buy"] + r["sell"]) if r else 0.0
                our_b += r["buy"] if r else 0.0
                ref_v += float(k[5])
                ref_b += float(k[9])
                n += 1
            if n == 0:
                print(f"{market + ' ' + sym:<14} пока нет полных минут, подожди 2-3 минуты")
                ok_all = False
                continue
            dv, db = pct(our_v, ref_v), pct(our_b, ref_b)
            ok = abs(dv) < 1 and abs(db) < 1
            ok_all &= ok
            print(f"{market + ' ' + sym:<14}{int(n):>6}{our_v:>14.3f}{ref_v:>14.3f}{dv:>8.2f}{db:>12.2f}  {'OK' if ok else 'ОШИБКА'}")
    hb = await con.fetch(
        "SELECT exchange, market, symbol, trades, round(extract(epoch FROM now() - last_trade)) AS ago "
        "FROM heartbeat ORDER BY 1, 2, 3")
    print("\nпульс потоков (сделок с запуска, секунд с последней сделки):")
    for r in hb:
        flag = "" if r["ago"] is not None and r["ago"] < 60 else "  <-- тишина"
        print(f"  {r['exchange']:<9}{r['market']:<5}{r['symbol']:<4}{r['trades']:>9}{int(r['ago'] or -1):>6}{flag}")
    rows = await con.fetchval("SELECT count(*) FROM fp_1m")
    size = await con.fetchval("SELECT pg_size_pretty(pg_total_relation_size('fp_1m'))")
    print(f"\nfp_1m: {rows} строк, {size}. Время сервера: {datetime.now(timezone.utc):%H:%M:%S} UTC")
    print("ИТОГ:", "всё сходится" if ok_all else "есть расхождения, смотри выше")
    await con.close()


if __name__ == "__main__":
    asyncio.run(main())
