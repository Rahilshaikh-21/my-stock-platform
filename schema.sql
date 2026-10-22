CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE market_region AS ENUM ('IN', 'US');
CREATE TYPE exchange_code AS ENUM ('NSE', 'BSE', 'NYSE', 'NASDAQ', 'AMEX');
CREATE TYPE asset_class AS ENUM ('EQUITY', 'ETF');
CREATE TYPE data_freshness_mode AS ENUM ('REAL_TIME', 'DELAYED_15M', 'END_OF_DAY', 'LAST_AVAILABLE', 'DEMO_HISTORICAL');
CREATE TYPE strategy_type AS ENUM ('LONG_TERM_VALUE', 'SWING_TRADING');
CREATE TYPE signal_action AS ENUM ('STRONG_BUY', 'BUY', 'HOLD', 'WAIT', 'TRIM_PARTIAL_SELL', 'SELL', 'AVOID', 'INSUFFICIENT_DATA');

CREATE TABLE instruments (
    instrument_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    country market_region NOT NULL,
    exchange exchange_code NOT NULL,
    asset_type asset_class NOT NULL,
    currency VARCHAR(3) NOT NULL CHECK (currency IN ('INR', 'USD')),
    sector VARCHAR(100),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    data_freshness data_freshness_mode NOT NULL DEFAULT 'REAL_TIME',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange)
);
