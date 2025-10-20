-- ============================================
-- Data Warehouse Query Examples
-- ============================================

-- 1. DAILY ANALYSIS - Get daily OHLC for specific stock
SELECT 
    symbol_code,
    trading_day,
    day_open,
    day_high,
    day_low,
    day_close,
    total_volume
FROM mv_daily_ohlc_summary
WHERE symbol_code = 'AAPL'
    AND asset_type_code = 'STOCK'
    AND trading_day >= NOW() - INTERVAL '30 days'
ORDER BY trading_day DESC;

-- 2. HOURLY ANALYSIS - Hourly price movement for forex pair
SELECT 
    symbol_code,
    trading_hour,
    hour_open,
    hour_high,
    hour_low,
    hour_close
FROM mv_hourly_ohlc_summary
WHERE symbol_code = 'EUR/USD'
    AND asset_type_code = 'FOREX'
    AND trading_hour >= NOW() - INTERVAL '24 hours'
ORDER BY trading_hour DESC;

-- 3. VOLATILITY ANALYSIS - Top 10 most volatile symbols
SELECT 
    symbol_code,
    asset_type_code,
    price_volatility,
    avg_price,
    all_time_high,
    all_time_low
FROM mv_symbol_analytics
WHERE price_volatility IS NOT NULL
ORDER BY price_volatility DESC
LIMIT 10;

-- 4. CROSS-ASSET COMPARISON
SELECT 
    f.timestamp,
    s.symbol_code,
    at.asset_type_code,
    f.close,
    f.volume
FROM fact_ohlc_data f
JOIN dim_symbol s ON f.symbol_id = s.symbol_id
JOIN dim_asset_type at ON f.asset_type_id = at.asset_type_id
WHERE s.symbol_code IN ('AAPL', 'EUR/USD', 'BTC')
    AND f.timestamp >= NOW() - INTERVAL '7 days'
ORDER BY f.timestamp DESC;

-- 5. VOLUME TRENDS - Top symbols by trading volume
SELECT 
    symbol_code,
    asset_type_code,
    total_volume_traded,
    total_records
FROM mv_symbol_analytics
WHERE total_volume_traded IS NOT NULL
ORDER BY total_volume_traded DESC
LIMIT 20;

-- 6. MOVING AVERAGES - Calculate 7-day and 30-day moving averages
WITH daily_prices AS (
    SELECT 
        symbol_code,
        trading_day,
        day_close,
        AVG(day_close) OVER (
            PARTITION BY symbol_code 
            ORDER BY trading_day 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as ma_7,
        AVG(day_close) OVER (
            PARTITION BY symbol_code 
            ORDER BY trading_day 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as ma_30
    FROM mv_daily_ohlc_summary
    WHERE symbol_code = 'AAPL'
        AND asset_type_code = 'STOCK'
        AND trading_day >= NOW() - INTERVAL '90 days'
)
SELECT 
    symbol_code,
    trading_day,
    day_close,
    ROUND(ma_7::numeric, 2) as moving_avg_7_day,
    ROUND(ma_30::numeric, 2) as moving_avg_30_day
FROM daily_prices
ORDER BY trading_day DESC;

-- 7. ASSET TYPE SUMMARY - Compare all asset types
SELECT 
    asset_type_code,
    unique_symbols,
    total_records,
    earliest_data,
    latest_data
FROM mv_asset_type_summary
ORDER BY total_records DESC;

-- 8. CONTINUOUS AGGREGATES - Query daily continuous aggregate
SELECT 
    s.symbol_code,
    cagg.bucket as trading_day,
    cagg.open,
    cagg.high,
    cagg.low,
    cagg.close,
    cagg.volume
FROM cagg_daily_ohlc cagg
JOIN dim_symbol s ON cagg.symbol_id = s.symbol_id
WHERE s.symbol_code = 'AAPL'
    AND cagg.bucket >= NOW() - INTERVAL '30 days'
ORDER BY cagg.bucket DESC;
