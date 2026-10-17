-- Footprint: схема базы. Выполняется один раз при первом запуске Postgres.

-- Минутные кластеры. Одна строка = минута x уровень цены x биржа x рынок x монета.
-- Любой таймфрейм (5М, 1Ч, 4Ч) и шаг ($50, $100) собираются из этих ячеек запросом.
CREATE TABLE IF NOT EXISTS fp_1m (
    ts        timestamptz      NOT NULL,           -- начало минуты, UTC
    exchange  text             NOT NULL,           -- binance | bybit | okx | coinbase
    market    text             NOT NULL,           -- spot | perp
    symbol    text             NOT NULL,           -- BTC | ETH
    price     double precision NOT NULL,           -- нижняя граница уровня: BTC шаг $10, ETH шаг $1
    buy_vol   double precision NOT NULL DEFAULT 0, -- агрессивные покупки (taker buy), в монетах
    sell_vol  double precision NOT NULL DEFAULT 0, -- агрессивные продажи (taker sell), в монетах
    buy_cnt   integer          NOT NULL DEFAULT 0, -- число сделок-покупок
    sell_cnt  integer          NOT NULL DEFAULT 0, -- число сделок-продаж
    PRIMARY KEY (symbol, ts, exchange, market, price)
);

-- Крупные сделки (одна сделка >= $100k) для слоя Large Trades.
CREATE TABLE IF NOT EXISTS big_trades (
    ts        timestamptz      NOT NULL,
    exchange  text             NOT NULL,
    market    text             NOT NULL,
    symbol    text             NOT NULL,
    side      text             NOT NULL,           -- buy | sell (сторона агрессора)
    price     double precision NOT NULL,
    qty       double precision NOT NULL,           -- в монетах
    usd       double precision NOT NULL,
    trade_id  text
);
CREATE INDEX IF NOT EXISTS big_trades_symbol_ts ON big_trades (symbol, ts);

-- Пульс потоков: когда пришла последняя сделка от каждой биржи.
CREATE TABLE IF NOT EXISTS heartbeat (
    exchange   text        NOT NULL,
    market     text        NOT NULL,
    symbol     text        NOT NULL,
    last_trade timestamptz,                -- время последней сделки по часам биржи
    last_seen  timestamptz,                -- когда коллектор последний раз писал в базу
    trades     bigint      NOT NULL DEFAULT 0,  -- сделок с момента старта коллектора
    started    timestamptz,                -- когда коллектор стартовал
    PRIMARY KEY (exchange, market, symbol)
);
