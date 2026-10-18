"""Собирает дашборд Grafana из исходников: SQL ниже + код панели footprint_panel.js.

Запуск: python3 grafana/build_dashboard.py  ->  grafana/provisioning/dashboards/footprint.json
Править код панели надо в footprint_panel.js, а не в JSON.
"""
import json
import pathlib

HERE = pathlib.Path(__file__).parent
DS = {"type": "grafana-postgresql-datasource", "uid": "footprint"}

# Шаг цены. «авто» зависит от монеты и таймфрейма (минимум - шаг хранения: BTC $10, ETH $1).
# Та же таблица продублирована в footprint_panel.js (autoStep) - менять вместе.
# NULLIF, а не CASE ... ELSE '$step'::numeric: литерал 'auto'::numeric Postgres разбирает ещё до выполнения и падает.
STEP = """coalesce(nullif('$step', 'auto')::numeric,
    CASE WHEN '$symbol' = 'BTC' THEN (CASE '$tf' WHEN '1 hour' THEN 25 WHEN '4 hours' THEN 100 ELSE 10 END)
         ELSE (CASE '$tf' WHEN '1 hour' THEN 2 WHEN '4 hours' THEN 5 ELSE 1 END) END)"""

# A: кластеры. Минутные ячейки fp_1m склеиваем в бары таймфрейма ($tf) и уровни шага.
SQL_CLUSTERS = """
SELECT
  date_bin('$tf'::interval, ts, TIMESTAMPTZ '2001-01-01') AS time,
  floor(price / STEP) * STEP                         AS level,
  sum(sell_vol)                                      AS sell,
  sum(buy_vol)                                       AS buy
FROM fp_1m
WHERE $__timeFilter(ts)
  AND symbol = '$symbol'
  AND ('$market' = 'all' OR market = '$market')
  AND ('$exchange' = 'all' OR exchange = '$exchange')
GROUP BY 1, 2
ORDER BY 1, 2
""".strip().replace("STEP", STEP)

# B: свечи. Для «все биржи» берём свечи Binance; для «spot + perp» - перп; у Coinbase только spot.
SQL_CANDLES = """
SELECT
  date_bin('$tf'::interval, ts, TIMESTAMPTZ '2001-01-01') AS time,
  (array_agg(open  ORDER BY open_ts))[1]             AS open,
  max(high)                                          AS high,
  min(low)                                           AS low,
  (array_agg(close ORDER BY close_ts DESC))[1]       AS close
FROM ohlc_1m
WHERE $__timeFilter(ts)
  AND symbol = '$symbol'
  AND exchange = CASE WHEN '$exchange' = 'all' THEN 'binance' ELSE '$exchange' END
  AND market = CASE WHEN '$exchange' = 'coinbase' THEN 'spot'
                    WHEN '$market' = 'all' THEN 'perp' ELSE '$market' END
GROUP BY 1
ORDER BY 1
""".strip()

# C: стакан сейчас. Последний снимок каждого потока (не старше 5 минут), заявки сложены по уровням шага.
# В book_1m лежат только уровни, где стояло от $50k (BTC) / $20k (ETH); last_usd - объём в последнем снимке минуты.
# Coinbase стакан не собираем: при выборе Coinbase панель стакана пустая.
SQL_BOOK = """
WITH latest AS (
  SELECT exchange, market, max(ts) AS ts, max(last_snap) AS snap
  FROM book_meta_1m
  WHERE symbol = '$symbol' AND ts > now() - interval '5 minutes'
    AND ('$market' = 'all' OR market = '$market')
    AND ('$exchange' = 'all' OR exchange = '$exchange')
  GROUP BY 1, 2
)
SELECT
  CASE WHEN b.side = 'ask' THEN 1 ELSE 0 END         AS ask,
  floor(b.price / STEP) * STEP                       AS level,
  sum(b.last_usd)                                    AS usd,
  max(max(l.snap)) OVER ()                           AS snap
FROM book_1m b
JOIN latest l ON l.exchange = b.exchange AND l.market = b.market AND l.ts = b.ts
WHERE b.symbol = '$symbol' AND b.last_usd > 0
GROUP BY 1, 2
ORDER BY 2
""".strip().replace("STEP", STEP)

# Тепловая карта стакана (ob-6). Ячейка = отрезок времени x уровень цены, значение - сколько $ в среднем
# стояло в заявках на уровне (сумма по выбранным стаканам, обе стороны). Размеры ячейки подбираются под окно:
#   по времени ~180 столбцов (не меньше минуты), по цене ~120 строк в диапазоне свечей с запасом
#   max(30% диапазона, 0.25% цены) сверху и снизу. Те же правила продублированы в heatmap_panel.js (запас).
# Среднее за минуту: сумма avg_usd делится на число минут со снимками в отрезке (минута без строки
# на уровне = там стояло меньше порога хранения, считаем нулём).
HEAT_SPAN = """span AS (
  SELECT (SELECT min(s) FROM unnest(ARRAY[60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 86400]) s
          WHERE s >= ($__unixEpochTo() - $__unixEpochFrom()) / 180.0) AS sec
)"""
CANDLE_SRC = """symbol = '$symbol'
  AND exchange = CASE WHEN '$exchange' = 'all' THEN 'binance' ELSE '$exchange' END
  AND market = CASE WHEN '$exchange' = 'coinbase' THEN 'spot'
                    WHEN '$market' = 'all' THEN 'perp' ELSE '$market' END"""
BOOK_SRC = """symbol = '$symbol'
  AND ('$market' = 'all' OR market = '$market')
  AND ('$exchange' = 'all' OR exchange = '$exchange')"""
BIN = "date_bin(make_interval(secs => span.sec), ts, TIMESTAMPTZ '2001-01-01')"

# Диапазон и шаг строк карты: общий для ячеек стакана (H) и крупных сделок (T).
HEAT_BOX = f"""px AS (
  SELECT min(low) AS lo, max(high) AS hi FROM ohlc_1m WHERE $__timeFilter(ts) AND {CANDLE_SRC}
),
box AS (
  SELECT lo - pad AS y0, hi + pad AS y1,
         coalesce((SELECT min(s) FROM unnest(CASE WHEN '$symbol' = 'BTC'
                                                  THEN ARRAY[10, 20, 25, 50, 100, 250, 500, 1000]
                                                  ELSE ARRAY[1, 2, 5, 10, 25, 50, 100] END) s
                   WHERE s >= (hi - lo + 2 * pad) / 120),
                  CASE WHEN '$symbol' = 'BTC' THEN 1000 ELSE 100 END) AS step
  FROM (SELECT lo, hi, greatest((hi - lo) * 0.3, hi * 0.0025) AS pad FROM px) q
)"""

SQL_HEAT = f"""
WITH {HEAT_SPAN},
{HEAT_BOX},
mins AS (
  SELECT {BIN} AS t, count(DISTINCT ts) AS n
  FROM book_meta_1m, span
  WHERE $__timeFilter(ts) AND {BOOK_SRC}
  GROUP BY 1
),
cells AS (
  SELECT {BIN} AS t, floor(price / box.step) * box.step AS level, sum(avg_usd) AS usd
  FROM book_1m, span, box
  WHERE $__timeFilter(ts) AND {BOOK_SRC}
    AND price >= box.y0 - box.step AND price < box.y1 + box.step
  GROUP BY 1, 2
)
SELECT c.t AS time, c.level, c.usd / m.n AS usd,
       (SELECT step FROM box) AS step, (SELECT sec FROM span) AS sec
FROM cells c JOIN mins m ON m.t = c.t
ORDER BY 1, 2
""".strip()

# Линия цены для тепловой карты: закрытие каждого отрезка (источник тот же, что у свечей footprint).
SQL_HEAT_PRICE = f"""
WITH {HEAT_SPAN}
SELECT {BIN} AS time,
  (array_agg(close ORDER BY close_ts DESC))[1]       AS close,
  max(high)                                          AS high,
  min(low)                                           AS low,
  max(span.sec)                                      AS sec
FROM ohlc_1m, span
WHERE $__timeFilter(ts) AND {CANDLE_SRC}
GROUP BY 1
ORDER BY 1
""".strip()

# Крупные сделки поверх карты (tr-1). Берём из big_trades рыночные заявки от $big (переменная дашборда;
# сборщик пишет туда заявки от $100k, заявка = все исполнения с одним временем и стороной - с 18.10, раньше
# Bybit и Coinbase писали каждое исполнение отдельно), складываем по ячейке карты (отрезок времени x строка
# цены) и стороне агрессора: сумма $, число заявок, средняя цена (взвешенная по $), с каких бирж.
SQL_HEAT_TRADES = f"""
WITH {HEAT_SPAN},
{HEAT_BOX}
SELECT {BIN}                                         AS time,
  CASE WHEN side = 'buy' THEN 1 ELSE 0 END           AS buy,
  sum(usd * price) / sum(usd)                        AS price,
  sum(usd)                                           AS usd,
  count(*)                                           AS n,
  string_agg(DISTINCT exchange || ' ' || market, ', ') AS src
FROM big_trades, span, box
WHERE $__timeFilter(ts) AND {BOOK_SRC}
  AND usd >= $big
  AND price >= box.y0 AND price < box.y1
GROUP BY 1, 2, floor(price / box.step)
ORDER BY 1
""".strip()

# Пульс: сколько секунд назад была последняя сделка по каждому потоку.
SQL_PULSE = """
SELECT exchange AS "биржа", market AS "рынок", symbol AS "монета",
       round(extract(epoch FROM now() - last_trade)) AS "сек с последней сделки",
       trades AS "сделок с запуска"
FROM heartbeat
ORDER BY 1, 2, 3
""".strip()


def custom_var(name, label, pairs, default):
    """Переменная-список. pairs: [(текст, значение)]."""
    opts = [{"text": t, "value": v, "selected": v == default} for t, v in pairs]
    cur = next(o for o in opts if o["selected"])
    return {
        "type": "custom", "name": name, "label": label, "hide": 0,
        "query": ",".join(f"{t} : {v}" for t, v in pairs),
        "current": {"text": cur["text"], "value": cur["value"], "selected": True},
        "options": opts, "multi": False, "includeAll": False, "skipUrlSync": False,
    }


def target(ref, sql):
    return {"refId": ref, "datasource": DS, "rawQuery": True, "editorMode": "code", "format": "table", "rawSql": sql}


code = (HERE / "footprint_panel.js").read_text(encoding="utf-8")
heat_code = (HERE / "heatmap_panel.js").read_text(encoding="utf-8")

dashboard = {
    "uid": "footprint",
    "title": "Footprint · кластеры",
    "tags": ["footprint"],
    "timezone": "utc",
    "editable": True,
    "graphTooltip": 0,
    "refresh": "10s",
    "time": {"from": "now-3h", "to": "now"},
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
    "schemaVersion": 39,
    "templating": {"list": [
        custom_var("symbol", "Монета", [("BTC", "BTC"), ("ETH", "ETH")], "BTC"),
        custom_var("market", "Рынок", [("spot + perp", "all"), ("spot", "spot"), ("perp", "perp")], "all"),
        custom_var("exchange", "Биржа", [("все", "all"), ("Binance", "binance"), ("Bybit", "bybit"),
                                          ("OKX", "okx"), ("Coinbase", "coinbase")], "all"),
        custom_var("tf", "Таймфрейм", [("1m", "1 minute"), ("5m", "5 minutes"), ("15m", "15 minutes"),
                                       ("1h", "1 hour"), ("4h", "4 hours")], "15 minutes"),
        custom_var("step", "Шаг цены, $", [("авто", "auto")] + [(s, s) for s in ("1", "2", "5", "10", "25", "50", "100",
                                                                               "250", "500")], "auto"),
        custom_var("heat", "Цвет карты", [("стены", "rel"), ("объём $", "abs")], "rel"),
        custom_var("big", "Сделки от, $", [("100k", "100000"), ("250k", "250000"), ("500k", "500000"),
                                           ("1M", "1000000"), ("5M", "5000000")], "500000"),
    ]},
    "panels": [
        {
            "id": 1, "type": "volkovlabs-echarts-panel", "datasource": DS,
            "title": "$symbol · footprint · Sell | Buy | Delta (в монетах), свечи: Binance perp для «все» · справа: стакан сейчас (Binance, Bybit, OKX)",
            "gridPos": {"h": 27, "w": 24, "x": 0, "y": 0},
            "targets": [target("A", SQL_CLUSTERS), target("B", SQL_CANDLES), target("C", SQL_BOOK)],
            "options": {
                "renderer": "canvas",
                "editorMode": "code",
                "getOption": code,
                "editor": {"format": "auto", "height": 600},
                "followTheme": False,   # цвета задаёт сам код панели (тёмная и светлая тема)
                "themeEditor": {"name": "default", "config": "{}"},
                "map": "none",
            },
        },
        {
            "id": 3, "type": "volkovlabs-echarts-panel", "datasource": DS,
            "title": "$symbol · тепловая карта стакана (цвет «стены»: во сколько раз больше обычного на таком расстоянии от цены; «объём $»: сколько стояло), жёлтая линия - цена, кружки - рыночные заявки от $${big:text}: зелёные покупки, красные продажи, площадь - сумма",
            "gridPos": {"h": 20, "w": 24, "x": 0, "y": 27},
            "targets": [target("H", SQL_HEAT), target("L", SQL_HEAT_PRICE), target("T", SQL_HEAT_TRADES)],
            "options": {
                "renderer": "canvas",
                "editorMode": "code",
                "getOption": heat_code,
                "editor": {"format": "auto", "height": 600},
                "followTheme": False,
                "themeEditor": {"name": "default", "config": "{}"},
                "map": "none",
            },
        },
        {
            "id": 2, "type": "table", "datasource": DS, "title": "Пульс потоков",
            "gridPos": {"h": 9, "w": 24, "x": 0, "y": 47},
            "targets": [target("A", SQL_PULSE)],
            "fieldConfig": {
                "defaults": {"custom": {"align": "left"}},
                "overrides": [{
                    "matcher": {"id": "byName", "options": "сек с последней сделки"},
                    "properties": [
                        {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                            {"color": "green", "value": None}, {"color": "orange", "value": 30},
                            {"color": "red", "value": 120}]}},
                        {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                    ],
                }],
            },
            "options": {"showHeader": True, "cellHeight": "sm"},
        },
    ],
}

out = HERE / "provisioning" / "dashboards" / "footprint.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("written", out)
