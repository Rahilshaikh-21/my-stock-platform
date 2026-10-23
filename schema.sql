-- ============================================================================
-- INSTITUTIONAL VALUE INVESTING & SWING TRADING AI PLATFORM - DATABASE SCHEMA
-- PostgreSQL 15+ Schema with Provenance, Discrepancy Audit & Ledger Tracking
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DO $$ BEGIN
    CREATE TYPE market_region AS ENUM ('IN', 'US');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE exchange_code AS ENUM ('NSE', 'BSE', 'NYSE', 'NASDAQ', 'AMEX');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE asset_class AS ENUM ('EQUITY', 'ETF');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE data_freshness_mode AS ENUM ('REAL_TIME', 'DELAYED_15M', 'END_OF_DAY', 'LAST_AVAILABLE', 'STALE', 'UNVERIFIED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE strategy_type AS ENUM ('LONG_TERM_VALUE', 'SWING_TRADING');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE signal_action AS ENUM (
        'STRONG_BUY',
        'BUY',
        'BUY_GARP',
        'HOLD',
        'WAIT',
        'TRIM_PARTIAL_SELL',
        'SELL',
        'AVOID',
        'INSUFFICIENT_DATA'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE verification_status AS ENUM (
        'VERIFIED',
        'DERIVED_VERIFIED',
        'DISCREPANCY_CORRECTED',
        'STALE',
        'UNVERIFIED',
        'MISSING',
        'INVALID_REJECTED'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE reporting_basis AS ENUM ('CONSOLIDATED', 'STANDALONE', 'TTM', 'ANNUAL_FY', 'QUARTERLY_MRQ', 'INDEX_NAV');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE transaction_type AS ENUM ('BUY', 'SELL', 'DIVIDEND', 'SPLIT', 'BONUS');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 1. Master Instruments Table
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    country market_region NOT NULL,
    exchange exchange_code NOT NULL,
    asset_type asset_class NOT NULL,
    currency VARCHAR(3) NOT NULL CHECK (currency IN ('INR', 'USD')),
    sector VARCHAR(100),
    industry VARCHAR(120),
    is_financial_sector BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    data_freshness data_freshness_mode NOT NULL DEFAULT 'REAL_TIME',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange)
);

CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments (symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_country_asset ON instruments (country, asset_type);

-- 2. Daily Price & OHLCV History Table
CREATE TABLE IF NOT EXISTS price_history_daily (
    price_id BIGSERIAL PRIMARY KEY,
    instrument_id UUID NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    trading_date DATE NOT NULL,
    open_price NUMERIC(18, 4) NOT NULL CHECK (open_price > 0),
    high_price NUMERIC(18, 4) NOT NULL CHECK (high_price > 0),
    low_price NUMERIC(18, 4) NOT NULL CHECK (low_price > 0),
    close_price NUMERIC(18, 4) NOT NULL CHECK (close_price > 0),
    adj_close_price NUMERIC(18, 4) NOT NULL CHECK (adj_close_price > 0),
    volume BIGINT NOT NULL DEFAULT 0 CHECK (volume >= 0),
    provider VARCHAR(64) NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (high_price >= low_price AND high_price >= close_price AND low_price <= close_price),
    UNIQUE (instrument_id, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_price_history_inst_date ON price_history_daily (instrument_id, trading_date DESC);

-- 3. Validated Fundamental Metrics & Provenance Table
CREATE TABLE IF NOT EXISTS fundamental_snapshots (
    snapshot_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
    basis reporting_basis NOT NULL DEFAULT 'CONSOLIDATED',
    currency VARCHAR(3) NOT NULL CHECK (currency IN ('INR', 'USD')),
    source_provider VARCHAR(64) NOT NULL,
    status verification_status NOT NULL DEFAULT 'UNVERIFIED',
    market_cap NUMERIC(22, 2),
    enterprise_value NUMERIC(22, 2),
    shares_outstanding NUMERIC(20, 2),
    revenue_ttm NUMERIC(22, 2),
    ebitda_ttm NUMERIC(22, 2),
    operating_profit_ttm NUMERIC(22, 2),
    net_profit_ttm NUMERIC(22, 2),
    eps_ttm NUMERIC(18, 4),
    diluted_eps_ttm NUMERIC(18, 4),
    forward_eps NUMERIC(18, 4),
    pe_ratio NUMERIC(12, 4),
    forward_pe NUMERIC(12, 4),
    industry_pe NUMERIC(12, 4),
    book_value_per_share NUMERIC(18, 4),
    pb_ratio NUMERIC(12, 4),
    ps_ratio NUMERIC(12, 4),
    ev_ebitda NUMERIC(12, 4),
    roe_pct NUMERIC(10, 4),
    roce_pct NUMERIC(10, 4),
    roic_pct NUMERIC(10, 4),
    roa_pct NUMERIC(10, 4),
    debt_to_equity NUMERIC(12, 4),
    interest_coverage NUMERIC(12, 4),
    operating_cash_flow NUMERIC(22, 2),
    free_cash_flow NUMERIC(22, 2),
    working_capital NUMERIC(22, 2),
    dividend_yield_pct NUMERIC(10, 4),
    payout_ratio_pct NUMERIC(10, 4),
    revenue_growth_pct NUMERIC(10, 4),
    earnings_growth_pct NUMERIC(10, 4),
    operating_margin_pct NUMERIC(10, 4),
    net_margin_pct NUMERIC(10, 4),
    beta NUMERIC(8, 4),
    wacc_pct NUMERIC(8, 4),
    cost_of_equity_pct NUMERIC(8, 4),
    provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fundamentals_inst_date ON fundamental_snapshots (instrument_id, retrieved_at DESC);

-- 4. AI Discrepancy Detection & Verification Audit Log
CREATE TABLE IF NOT EXISTS discrepancy_audit_log (
    discrepancy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID REFERENCES instruments(instrument_id) ON DELETE SET NULL,
    symbol VARCHAR(32) NOT NULL,
    metric_name VARCHAR(64) NOT NULL,
    discrepancy_code VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    reported_value NUMERIC(22, 6),
    recalculated_value NUMERIC(22, 6),
    corrected_value NUMERIC(22, 6),
    correction_applied BOOLEAN NOT NULL DEFAULT FALSE,
    likely_cause TEXT NOT NULL,
    verification_evidence TEXT NOT NULL,
    confidence_pct NUMERIC(5, 2) NOT NULL DEFAULT 0.0,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_discrepancy_symbol_date ON discrepancy_audit_log (symbol, detected_at DESC);

-- 5. Investment & Swing Signals Audit Table
CREATE TABLE IF NOT EXISTS investment_signals (
    signal_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument_id UUID NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    strategy strategy_type NOT NULL,
    action signal_action NOT NULL,
    confidence_pct NUMERIC(5, 2) NOT NULL CHECK (confidence_pct >= 0 AND confidence_pct <= 100),
    price_at_signal NUMERIC(18, 4) NOT NULL CHECK (price_at_signal > 0),
    intrinsic_value_low NUMERIC(18, 4),
    intrinsic_fair_value NUMERIC(18, 4),
    intrinsic_value_high NUMERIC(18, 4),
    safe_buy_limit NUMERIC(18, 4),
    sell_trigger_limit NUMERIC(18, 4),
    margin_of_safety_pct NUMERIC(10, 2),
    stop_loss NUMERIC(18, 4),
    profit_target NUMERIC(18, 4),
    risk_reward_ratio NUMERIC(10, 2),
    quality_score NUMERIC(5, 2),
    valuation_score NUMERIC(5, 2),
    health_score NUMERIC(5, 2),
    momentum_score NUMERIC(5, 2),
    data_quality_score NUMERIC(5, 2),
    supporting_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    contradictory_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    major_risks JSONB NOT NULL DEFAULT '[]'::jsonb,
    assumptions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_signals_inst_strategy ON investment_signals (instrument_id, strategy, generated_at DESC);

-- 6. Portfolio Transactions Ledger (Weighted-Average Cost Basis & Deduplication)
CREATE TABLE IF NOT EXISTS portfolio_transactions (
    tx_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tx_fingerprint VARCHAR(64) NOT NULL UNIQUE,
    symbol VARCHAR(32) NOT NULL,
    exchange exchange_code NOT NULL DEFAULT 'NSE',
    tx_type transaction_type NOT NULL,
    tx_date DATE NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL CHECK (quantity > 0),
    price_per_unit NUMERIC(18, 4) NOT NULL DEFAULT 0.0 CHECK (price_per_unit >= 0),
    fees NUMERIC(18, 4) NOT NULL DEFAULT 0.0 CHECK (fees >= 0),
    taxes NUMERIC(18, 4) NOT NULL DEFAULT 0.0 CHECK (taxes >= 0),
    split_ratio NUMERIC(12, 6) DEFAULT 1.0,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR' CHECK (currency IN ('INR', 'USD')),
    source VARCHAR(64) NOT NULL DEFAULT 'MANUAL',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_tx_symbol_date ON portfolio_transactions (symbol, tx_date ASC);

-- 7. Portfolio Holdings Summary
CREATE TABLE IF NOT EXISTS portfolio_holdings (
    holding_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(32) NOT NULL,
    exchange exchange_code NOT NULL DEFAULT 'NSE',
    quantity NUMERIC(18, 4) NOT NULL CHECK (quantity > 0),
    avg_buy_price NUMERIC(18, 4) CHECK (avg_buy_price IS NULL OR avg_buy_price >= 0),
    total_cost_basis NUMERIC(20, 4),
    realized_pnl NUMERIC(20, 4) NOT NULL DEFAULT 0.0,
    realized_dividends NUMERIC(20, 4) NOT NULL DEFAULT 0.0,
    total_fees_paid NUMERIC(18, 4) NOT NULL DEFAULT 0.0,
    target_weight_pct NUMERIC(6, 2),
    cost_basis_verified BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange)
);

-- 8. Watchlist Table
CREATE TABLE IF NOT EXISTS watchlist_items (
    watchlist_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(32) NOT NULL,
    exchange exchange_code NOT NULL DEFAULT 'NSE',
    desired_entry NUMERIC(18, 4) DEFAULT 0.0,
    thesis TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange)
);
