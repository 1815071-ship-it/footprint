"""Собирает дашборд Grafana из исходников: SQL ниже + код панели footprint_panel.js.

Запуск: python3 grafana/build_dashboard.py  ->  grafana/provisioning/dashboards/footprint.json
Править код панели надо в footprint_panel.js, а не в JSON.
"""
import json
import pathlib

HERE = pathlib.Path(__file__).parent
DS = {"type": "grafana-postgresql-datasource", "uid": "footprint"}

# A: кластеры. Минутные ячейки fp_1m склеиваем в бары таймфрейма ($tf) и уровни шага ($step).
SQL_CLUSTERS = """
SELECT
  date_bin('$tf'::interval, ts, TIMESTAMPTZ '2001-01-01') AS time,
  floor(price / $step) * $step                       AS level,
  sum(sell_vol)                                      AS sell,
  sum(buy_vol)                                       AS buy
FROM fp_1m
WHERE $__timeFilter(ts)
  AND symbol = '$symbol'
  AND ('$market' = 'all' OR market = '$market')
  AND ('$exchange' = 'all' OR exchange = '$exchange')
GROUP BY 1, 2
ORDER BY 1, 2
""".strip()

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
        custom_var("step", "Шаг цены, $", [(s, s) for s in ("1", "2", "5", "10", "25", "50", "100", "250", "500")], "50"),
    ]},
    "panels": [
        {
            "id": 1, "type": "volkovlabs-echarts-panel", "datasource": DS,
            "title": "$symbol · footprint · Sell | Buy | Delta (в монетах), свечи: Binance perp для «все»",
            "gridPos": {"h": 27, "w": 24, "x": 0, "y": 0},
            "targets": [target("A", SQL_CLUSTERS), target("B", SQL_CANDLES)],
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
            "id": 2, "type": "table", "datasource": DS, "title": "Пульс потоков",
            "gridPos": {"h": 9, "w": 24, "x": 0, "y": 27},
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
