-- ============================================
-- Data Warehouse Schema for Time-Series Data
-- ============================================
-- This schema consolidates stock, forex, and crypto data
-- into a unified star schema for analytical queries
--
-- Components:
-- 1. Dimension Tables (dim_*)
-- 2. Fact Table (fact_ohlc_data)
-- 3. Materialized Views (mv_*)
-- 4. Continuous Aggregates (cagg_*)
-- 5. Indexes for Performance
-- ============================================

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Asset Type Dimension
CREATE TABLE IF NOT EXISTS dim_asset_type (
    asset_type_id SERIAL PRIMARY KEY,
    asset_type_code VARCHAR(10) NOT NULL UNIQUE,
    asset_type_name VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert standard asset types
INSERT INTO dim_asset_type (asset_type_code, asset_type_name, description)
VALUES 
    ('STOCK', 'Stock', 'Equity securities'),
    ('FOREX', 'Forex', 'Foreign exchange currency pairs'),
    ('CRYPTO', 'Cryptocurrency', 'Digital cryptocurrencies')
ON CONFLICT (asset_type_code) DO NOTHING;

-- Symbol Dimension (stocks, forex pairs, crypto symbols)
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id SERIAL PRIMARY KEY,
    symbol_code VARCHAR(20) NOT NULL,
    asset_type_id INTEGER NOT NULL REFERENCES dim_asset_type(asset_type_id),
    symbol_name VARCHAR(100),
    exchange VARCHAR(50),
    currency VARCHAR(10),
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol_code, asset_type_id)
);

-- Time Interval Dimension
CREATE TABLE IF NOT EXISTS dim_time_interval (
    interval_id SERIAL PRIMARY KEY,
    interval_code VARCHAR(10) NOT NULL UNIQUE,
    interval_name VARCHAR(50) NOT NULL,
    interval_minutes INTEGER NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Insert standard time intervals
INSERT INTO dim_time_interval (interval_code, interval_name, interval_minutes, description)
VALUES 
    ('1min', '1 Minute', 1, 'One minute interval'),
    ('5min', '5 Minutes', 5, 'Five minutes interval'),
    ('15min', '15 Minutes', 15, 'Fifteen minutes interval'),
    ('1h', '1 Hour', 60, 'One hour interval'),
    ('4h', '4 Hours', 240, 'Four hours interval'),
    ('1day', '1 Day', 1440, 'Daily interval')
ON CONFLICT (interval_code) DO NOTHING;

-- Exchange Dimension
CREATE TABLE IF NOT EXISTS dim_exchange (
    exchange_id SERIAL PRIMARY KEY,
    exchange_code VARCHAR(20) NOT NULL UNIQUE,
    exchange_name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    timezone VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- FACT TABLE
-- ============================================

-- Unified OHLC Fact Table
CREATE TABLE IF NOT EXISTS fact_ohlc_data (
    fact_id BIGSERIAL,
    symbol_id INTEGER NOT NULL REFERENCES dim_symbol(symbol_id),
    asset_type_id INTEGER NOT NULL REFERENCES dim_asset_type(asset_type_id),
    interval_id INTEGER NOT NULL REFERENCES dim_time_interval(interval_id),
    timestamp TIMESTAMPTZ NOT NULL,
    open DECIMAL NOT NULL,
    high DECIMAL NOT NULL,
    low DECIMAL NOT NULL,
    close DECIMAL NOT NULL,
    volume DECIMAL,
    volume_from DECIMAL,
    volume_to DECIMAL,
    price_change DECIMAL,
    avg_price DECIMAL,
    source_table VARCHAR(50),
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fact_id, timestamp)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('fact_ohlc_data', 'timestamp',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Dimension table indexes
CREATE INDEX IF NOT EXISTS idx_dim_symbol_asset_type ON dim_symbol(asset_type_id);
CREATE INDEX IF NOT EXISTS idx_dim_symbol_code ON dim_symbol(symbol_code);
CREATE INDEX IF NOT EXISTS idx_dim_symbol_active ON dim_symbol(is_active) WHERE is_active = true;

-- Fact table indexes for time-range queries
CREATE INDEX IF NOT EXISTS idx_fact_ohlc_timestamp ON fact_ohlc_data(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_ohlc_symbol_timestamp ON fact_ohlc_data(symbol_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_ohlc_asset_type_timestamp ON fact_ohlc_data(asset_type_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_ohlc_interval_timestamp ON fact_ohlc_data(interval_id, timestamp DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_ohlc_symbol_interval_timestamp ON fact_ohlc_data(symbol_id, interval_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_ohlc_asset_symbol_timestamp ON fact_ohlc_data(asset_type_id, symbol_id, timestamp DESC);

-- ============================================
-- MATERIALIZED VIEWS
-- ============================================

-- Daily OHLC Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_ohlc_summary AS
SELECT 
    s.symbol_code,
    at.asset_type_code,
    ti.interval_code,
    DATE_TRUNC('day', f.timestamp) as trading_day,
    COUNT(*) as record_count,
    MIN(f.open) as day_open,
    MAX(f.high) as day_high,
    MIN(f.low) as day_low,
    MAX(f.close) as day_close,
    SUM(f.volume) as total_volume,
    AVG(f.close) as avg_close_price,
    MAX(f.timestamp) as last_updated
FROM fact_ohlc_data f
JOIN dim_symbol s ON f.symbol_id = s.symbol_id
JOIN dim_asset_type at ON f.asset_type_id = at.asset_type_id
JOIN dim_time_interval ti ON f.interval_id = ti.interval_id
GROUP BY s.symbol_code, at.asset_type_code, ti.interval_code, DATE_TRUNC('day', f.timestamp)
WITH DATA;

-- Hourly OHLC Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_ohlc_summary AS
SELECT 
    s.symbol_code,
    at.asset_type_code,
    ti.interval_code,
    DATE_TRUNC('hour', f.timestamp) as trading_hour,
    COUNT(*) as record_count,
    FIRST(f.open, f.timestamp) as hour_open,
    MAX(f.high) as hour_high,
    MIN(f.low) as hour_low,
    LAST(f.close, f.timestamp) as hour_close,
    SUM(f.volume) as total_volume,
    AVG(f.close) as avg_close_price
FROM fact_ohlc_data f
JOIN dim_symbol s ON f.symbol_id = s.symbol_id
JOIN dim_asset_type at ON f.asset_type_id = at.asset_type_id
JOIN dim_time_interval ti ON f.interval_id = ti.interval_id
GROUP BY s.symbol_code, at.asset_type_code, ti.interval_code, DATE_TRUNC('hour', f.timestamp)
WITH DATA;

-- Asset Type Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_asset_type_summary AS
SELECT 
    at.asset_type_code,
    at.asset_type_name,
    COUNT(DISTINCT s.symbol_id) as unique_symbols,
    COUNT(*) as total_records,
    MIN(f.timestamp) as earliest_data,
    MAX(f.timestamp) as latest_data
FROM fact_ohlc_data f
JOIN dim_asset_type at ON f.asset_type_id = at.asset_type_id
JOIN dim_symbol s ON f.symbol_id = s.symbol_id
GROUP BY at.asset_type_code, at.asset_type_name
WITH DATA;

-- Symbol Analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_symbol_analytics AS
SELECT 
    s.symbol_code,
    at.asset_type_code,
    s.exchange,
    COUNT(*) as total_records,
    MIN(f.timestamp) as first_record_date,
    MAX(f.timestamp) as last_record_date,
    AVG(f.close) as avg_price,
    STDDEV(f.close) as price_volatility,
    MIN(f.low) as all_time_low,
    MAX(f.high) as all_time_high,
    SUM(f.volume) as total_volume_traded
FROM fact_ohlc_data f
JOIN dim_symbol s ON f.symbol_id = s.symbol_id
JOIN dim_asset_type at ON f.asset_type_id = at.asset_type_id
GROUP BY s.symbol_code, at.asset_type_code, s.exchange
WITH DATA;

-- Indexes on materialized views
CREATE INDEX IF NOT EXISTS idx_mv_daily_symbol_day ON mv_daily_ohlc_summary(symbol_code, trading_day DESC);
CREATE INDEX IF NOT EXISTS idx_mv_daily_asset_day ON mv_daily_ohlc_summary(asset_type_code, trading_day DESC);
CREATE INDEX IF NOT EXISTS idx_mv_hourly_symbol_hour ON mv_hourly_ohlc_summary(symbol_code, trading_hour DESC);
CREATE INDEX IF NOT EXISTS idx_mv_symbol_analytics_symbol ON mv_symbol_analytics(symbol_code);

-- ============================================
-- CONTINUOUS AGGREGATES (TimescaleDB)
-- ============================================

-- Daily Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_daily_ohlc
WITH (timescaledb.continuous) AS
SELECT 
    symbol_id,
    asset_type_id,
    interval_id,
    time_bucket('1 day', timestamp) AS bucket,
    FIRST(open, timestamp) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST(close, timestamp) as close,
    SUM(volume) as volume,
    AVG(close) as avg_price,
    COUNT(*) as record_count
FROM fact_ohlc_data
GROUP BY symbol_id, asset_type_id, interval_id, bucket
WITH NO DATA;

-- Add refresh policy for daily aggregate (refresh every hour)
SELECT add_continuous_aggregate_policy('cagg_daily_ohlc',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Hourly Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_hourly_ohlc
WITH (timescaledb.continuous) AS
SELECT 
    symbol_id,
    asset_type_id,
    interval_id,
    time_bucket('1 hour', timestamp) AS bucket,
    FIRST(open, timestamp) as open,
    MAX(high) as high,
    MIN(low) as low,
    LAST(close, timestamp) as close,
    SUM(volume) as volume,
    AVG(close) as avg_price,
    COUNT(*) as record_count
FROM fact_ohlc_data
GROUP BY symbol_id, asset_type_id, interval_id, bucket
WITH NO DATA;

-- Add refresh policy for hourly aggregate (refresh every 15 minutes)
SELECT add_continuous_aggregate_policy('cagg_hourly_ohlc',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

-- ============================================
-- EXAMPLE QUERIES
-- ============================================

-- Query 1: Get daily OHLC for a specific symbol
-- SELECT 
--     symbol_code,
--     trading_day,
--     day_open,
--     day_high,
--     day_low,
--     day_close,
--     total_volume
-- FROM mv_daily_ohlc_summary
-- WHERE symbol_code = 'AAPL'
--     AND asset_type_code = 'STOCK'
--     AND trading_day >= NOW() - INTERVAL '30 days'
-- ORDER BY trading_day DESC;

-- Query 2: Compare performance across asset types
-- SELECT 
--     asset_type_code,
--     unique_symbols,
--     total_records,
--     earliest_data,
--     latest_data
-- FROM mv_asset_type_summary
-- ORDER BY total_records DESC;

-- Query 3: Get top volatile symbols
-- SELECT 
--     symbol_code,
--     asset_type_code,
--     price_volatility,
--     avg_price,
--     all_time_high,
--     all_time_low
-- FROM mv_symbol_analytics
-- WHERE price_volatility IS NOT NULL
-- ORDER BY price_volatility DESC
-- LIMIT 10;

-- ============================================
-- MAINTENANCE QUERIES
-- ============================================

-- Refresh all materialized views
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_ohlc_summary;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_ohlc_summary;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_asset_type_summary;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_symbol_analytics;

-- Check hypertable information
-- SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'fact_ohlc_data';

-- Check continuous aggregate policies
-- SELECT * FROM timescaledb_information.jobs WHERE proc_name LIKE '%continuous_aggregate%';

-- Analyze table statistics
-- ANALYZE fact_ohlc_data;
-- ANALYZE dim_symbol;
-- ANALYZE dim_asset_type;
-- ANALYZE dim_time_interval;
