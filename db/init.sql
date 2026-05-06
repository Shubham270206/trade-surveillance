-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Trades table partitioned by month
CREATE TABLE trades (
    trade_id    UUID DEFAULT uuid_generate_v4(),
    trader_id   VARCHAR(20)     NOT NULL,
    symbol      VARCHAR(10)     NOT NULL,
    side        VARCHAR(4)      NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity    INTEGER         NOT NULL CHECK (quantity > 0),
    price       NUMERIC(10, 2)  NOT NULL CHECK (price > 0),
    timestamp   TIMESTAMPTZ     NOT NULL,
    cancelled   BOOLEAN         NOT NULL DEFAULT FALSE,
    cancel_timestamp TIMESTAMPTZ,
    order_type  VARCHAR(10)     NOT NULL DEFAULT 'LIMIT',
    PRIMARY KEY (trade_id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions for the next 3 months
CREATE TABLE trades_2025_01 PARTITION OF trades
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE trades_2025_02 PARTITION OF trades
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE trades_2025_03 PARTITION OF trades
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE trades_2025_04 PARTITION OF trades
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE trades_2025_05 PARTITION OF trades
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE trades_2025_06 PARTITION OF trades
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

-- Alerts table
CREATE TABLE alerts (
    alert_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trader_id   VARCHAR(20)     NOT NULL,
    alert_type  VARCHAR(30)     NOT NULL CHECK (alert_type IN ('SPOOFING', 'WASH_TRADING', 'LAYERING', 'VOLUME_SPIKE', 'IMBALANCE', 'ANOMALY')),
    confidence  NUMERIC(5, 2)   NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    explanation TEXT            NOT NULL,
    triggered_at TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    trade_ids   UUID[]
);

-- Trader risk scores
CREATE TABLE trader_scores (
    trader_id   VARCHAR(20)     PRIMARY KEY,
    risk_score  NUMERIC(5, 2)   NOT NULL DEFAULT 0 CHECK (risk_score BETWEEN 0 AND 1),
    total_flags INTEGER         NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Indexes for fast analytical queries
CREATE INDEX idx_trades_trader_time   ON trades (trader_id, timestamp);
CREATE INDEX idx_trades_symbol_time   ON trades (symbol, timestamp);
CREATE INDEX idx_trades_cancelled     ON trades (cancelled, timestamp) WHERE cancelled = TRUE;
CREATE INDEX idx_alerts_type_time     ON alerts (alert_type, triggered_at);
CREATE INDEX idx_alerts_trader_time   ON alerts (trader_id, triggered_at);
CREATE INDEX idx_scores_risk          ON trader_scores (risk_score DESC);

-- Seed a few trader IDs so the table isn't empty on first query
INSERT INTO trader_scores (trader_id, risk_score, total_flags)
VALUES
    ('T_normal_001', 0.0, 0),
    ('T_normal_002', 0.0, 0),
    ('T_spoofer_001', 0.0, 0),
    ('T_wash_001',   0.0, 0);