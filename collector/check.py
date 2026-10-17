"""Сверка с независимыми источниками: наши кластеры против официальных данных бирж.

По каждому из 14 потоков (биржа x рынок x монета) берём полные минуты с момента старта коллектора:
  объём:   sum(buy_vol + sell_vol)  против объёма официальных 1m-свечей биржи (REST);
  покупки: sum(buy_vol)             против объёма агрессивных покупок (taker buy):
           Binance - поле taker buy base volume в свече;
           Bybit, OKX, Coinbase - сумма по ленте последних сделок (REST), только за минуты,
           которые лента покрывает целиком. У Coinbase в ленте сторона мейкера:
           side = "sell" значит, что агрессор купил.
Расхождение больше 1% = ошибка в коллекторе.
"""
import asyncio
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import asyncpg

TOL_PCT = 1.0
# Сколько секунд ждать после закрытия минуты, пока биржа её дособерёт. По умолчанию 10.
SETTLE = {"coinbase": 60}
STREAMS = [
    ("binance", "spot"), ("binance", "perp"),
    ("bybit", "spot"), ("bybit", "perp"),
    ("okx", "spot"), ("okx", "perp"),
    ("coinbase", "spot"),
]
SYMBOLS = ("BTC", "ETH")

# Наши минуты по одному потоку: покупки и весь объём на всех уровнях цены.
SQL = """
SELECT ts, sum(buy_vol) AS buy, sum(buy_vol + sell_vol) AS vol
FROM fp_1m
WHERE exchange = $1 AND market = $2 AND symbol = $3 AND ts > $4
GROUP BY ts
"""


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "footprint-check/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


def minute(ts_sec):
    return int(ts_sec) // 60 * 60


def pct(a, b):
    return 0.0 if b == 0 else (a - b) / b * 100


def iso_ms(s):
    """'2026-10-17T08:55:41.174188Z' -> миллисекунды UTC."""
    main, _, frac = s.rstrip("Z").partition(".")
    dt = datetime.strptime(main, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000 + int((frac + "000")[:3])


def candles(rows):
    """Свечи [(минута, объём)] -> ({минута: объём}, нижняя граница покрытия)."""
    vol = {}
    for m, v in rows:
        vol[m] = vol.get(m, 0.0) + v
    return vol, (min(vol) - 60) if vol else None


def tape(trades):
    """Лента [(время мс, количество, агрессор купил)] -> ({минута: покупки}, нижняя граница покрытия).
    Самую старую минуту ленты не считаем: лента могла начаться с её середины."""
    trades = list(trades)
    if not trades:
        return {}, None
    oldest = min(minute(t / 1000) for t, _, _ in trades)
    buy = {}
    for t, qty, is_buy in trades:
        m = minute(t / 1000)
        if m > oldest and is_buy:
            buy[m] = buy.get(m, 0.0) + qty
    return buy, oldest


def ref_binance(market, sym):
    base = "https://api.binance.com/api/v3" if market == "spot" else "https://fapi.binance.com/fapi/v1"
    rows = get(f"{base}/klines?symbol={sym}USDT&interval=1m&limit=60")
    # свеча: [время открытия мс, open, high, low, close, объём, ..., taker buy в монетах (индекс 9), ...]
    vol, vol_from = candles((int(k[0]) // 1000, float(k[5])) for k in rows)
    buy, _ = candles((int(k[0]) // 1000, float(k[9])) for k in rows)
    return vol, vol_from, buy, vol_from


def bybit(path):
    j = get("https://api.bybit.com/v5/market/" + path)
    if j.get("retCode") != 0:
        raise RuntimeError(f"bybit retCode {j.get('retCode')}: {j.get('retMsg')}")
    return j["result"]["list"]


def ref_bybit(market, sym):
    cat = "spot" if market == "spot" else "linear"
    # свеча: [время открытия мс, open, high, low, close, объём в монетах, оборот в USDT]
    vol, vol_from = candles((int(r[0]) // 1000, float(r[5]))
                            for r in bybit(f"kline?category={cat}&symbol={sym}USDT&interval=1&limit=60"))
    # лента: side = сторона агрессора. spot отдаёт максимум 60 последних сделок, linear - 1000
    lim = 60 if cat == "spot" else 1000
    buy, buy_from = tape((int(t["time"]), float(t["size"]), t["side"] == "Buy")
                         for t in bybit(f"recent-trade?category={cat}&symbol={sym}USDT&limit={lim}"))
    return vol, vol_from, buy, buy_from


def okx(path):
    j = get("https://www.okx.com/api/v5/" + path)
    if j.get("code") != "0":
        raise RuntimeError(f"okx code {j.get('code')}: {j.get('msg')}")
    return j["data"]


def ref_okx(market, sym):
    inst = f"{sym}-USDT" if market == "spot" else f"{sym}-USDT-SWAP"
    # свеча: [время мс, o, h, l, c, vol, volCcy, volCcyQuote, confirm].
    # spot: vol - объём в монетах. swap: vol - в контрактах, volCcy - в монетах
    col = 5 if market == "spot" else 6
    vol, vol_from = candles((int(r[0]) // 1000, float(r[col]))
                            for r in okx(f"market/candles?instId={inst}&bar=1m&limit=60"))
    ct = 1.0  # в ленте перпа количество в контрактах, переводим в монеты
    if market == "perp":
        ct = float(okx(f"public/instruments?instType=SWAP&instId={inst}")[0]["ctVal"])
    # лента: side = сторона агрессора, максимум 500 последних сделок
    buy, buy_from = tape((int(t["ts"]), float(t["sz"]) * ct, t["side"] == "buy")
                         for t in okx(f"market/trades?instId={inst}&limit=500"))
    return vol, vol_from, buy, buy_from


def ref_coinbase(market, sym):
    p = f"https://api.exchange.coinbase.com/products/{sym}-USD"
    # свеча: [время открытия с, low, high, open, close, объём в монетах]; минуты без сделок пропущены
    vol, vol_from = candles((int(r[0]), float(r[5])) for r in get(f"{p}/candles?granularity=60"))
    # лента: side = сторона мейкера, поэтому "sell" значит, что агрессор купил. Максимум 1000 сделок
    buy, buy_from = tape((iso_ms(t["time"]), float(t["size"]), t["side"] == "sell")
                         for t in get(f"{p}/trades?limit=1000"))
    return vol, vol_from, buy, buy_from


REF = {"binance": ref_binance, "bybit": ref_bybit, "okx": ref_okx, "coinbase": ref_coinbase}


def compare(ours, ref, ref_from, start, hi):
    """Суммы за полные минуты start < m <= hi, покрытые биржей: (минут, наше, биржа, разн.%) или None."""
    if ref_from is None:
        return None
    lo = max(start, ref_from)
    n = (hi - lo) // 60
    if n <= 0:
        return None
    o = sum(v for m, v in ours.items() if lo < m <= hi)
    r = sum(v for m, v in ref.items() if lo < m <= hi)
    return n, o, r, pct(o, r)


async def main():
    con = await asyncpg.connect(os.environ["PG_DSN"])
    started = await con.fetchval("SELECT max(started) FROM heartbeat")
    if started is None:
        print("коллектор ещё ничего не записал, подожди минуту")
        await con.close()
        return
    now = time.time()
    # минута старта неполная, её не сверяем; глубже 6 часов не смотрим
    start = max(minute(started.timestamp()), minute(now) - 6 * 3600)
    start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
    ok_all, unchecked = True, []
    print(f"сверка с официальными данными бирж с {start_dt:%H:%M} UTC (старт коллектора {started:%H:%M:%S}), допуск {TOL_PCT:g}%")
    print(f"{'поток':<18}{'минут':>6}{'наш объём':>13}{'биржа':>13}{'разн.%':>8}{'покупки: минут':>16}{'разн.%':>8}  итог")
    for exchange, market in STREAMS:
        hi = minute(now - 60 - SETTLE.get(exchange, 10))  # последняя минута, которую биржа уже дособрала
        for sym in SYMBOLS:
            name = f"{exchange} {market} {sym}"
            rows = await con.fetch(SQL, exchange, market, sym, start_dt)
            our_vol = {int(r["ts"].timestamp()): r["vol"] for r in rows}
            our_buy = {int(r["ts"].timestamp()): r["buy"] for r in rows}
            try:
                vol, vol_from, buy, buy_from = REF[exchange](market, sym)
            except Exception as e:  # noqa: BLE001
                print(f"{name:<18} биржа не ответила: {str(e)[:90]}")
                unchecked.append(name)
                continue
            v = compare(our_vol, vol, vol_from, start, hi)
            b = compare(our_buy, buy, buy_from, start, hi)
            if v is None:
                print(f"{name:<18} пока нет полных минут, подожди 2-3 минуты")
                unchecked.append(name)
                continue
            ok = abs(v[3]) < TOL_PCT and (b is None or abs(b[3]) < TOL_PCT)
            ok_all &= ok
            verdict = "ОШИБКА" if not ok else ("OK" if b else "OK, только объём")
            bcol = f"{b[0]:>16}{b[3]:>8.2f}" if b else f"{'лента коротка':>16}{'-':>8}"
            print(f"{name:<18}{v[0]:>6}{v[1]:>13.3f}{v[2]:>13.3f}{v[3]:>8.2f}{bcol}  {verdict}")
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
    if not ok_all:
        print("ИТОГ: есть расхождения, смотри выше")
    elif unchecked:
        print("ИТОГ: расхождений нет, но не проверено:", ", ".join(unchecked))
    else:
        print("ИТОГ: всё сходится")
    await con.close()


if __name__ == "__main__":
    asyncio.run(main())
