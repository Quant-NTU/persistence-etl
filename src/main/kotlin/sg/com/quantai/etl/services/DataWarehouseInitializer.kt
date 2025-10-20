package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class DataWarehouseInitializer(val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(DataWarehouseInitializer::class.java)

    @Bean(name = ["dataWarehouseInitializer"])
    fun runDataWarehouseInitializer(): CommandLineRunner {
        return CommandLineRunner {

            logger.info("Initializing data warehouse schema...")

            // Create dimension tables
            createDimensionTables()

            // Create fact tables
            createFactTables()

            // Create materialized views
            createMaterializedViews()

            // Create indexes for optimized queries
            createDataWarehouseIndexes()

            // Setup continuous aggregates and refresh policies
            setupContinuousAggregates()

            logger.info("Data warehouse schema initialization completed successfully.")
        }
    }

    private fun createDimensionTables() {
        logger.info("Creating dimension tables...")

        // Dimension table for asset types
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS dim_asset_type (
                asset_type_id SERIAL PRIMARY KEY,
                asset_type_code VARCHAR(10) NOT NULL UNIQUE,
                asset_type_name VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Table 'dim_asset_type' created.")

        // Insert standard asset types
        jdbcTemplate.execute("""
            INSERT INTO dim_asset_type (asset_type_code, asset_type_name, description)
            VALUES 
                ('STOCK', 'Stock', 'Equity securities'),
                ('FOREX', 'Forex', 'Foreign exchange currency pairs'),
                ('CRYPTO', 'Cryptocurrency', 'Digital cryptocurrencies')
            ON CONFLICT (asset_type_code) DO NOTHING;
        """)

        // Dimension table for symbols/currency pairs
        jdbcTemplate.execute("""
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
        """)
        logger.info("Table 'dim_symbol' created.")

        // Dimension table for time intervals
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS dim_time_interval (
                interval_id SERIAL PRIMARY KEY,
                interval_code VARCHAR(10) NOT NULL UNIQUE,
                interval_name VARCHAR(50) NOT NULL,
                interval_minutes INTEGER NOT NULL,
                description TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Table 'dim_time_interval' created.")

        // Insert standard time intervals
        jdbcTemplate.execute("""
            INSERT INTO dim_time_interval (interval_code, interval_name, interval_minutes, description)
            VALUES 
                ('1min', '1 Minute', 1, 'One minute interval'),
                ('5min', '5 Minutes', 5, 'Five minutes interval'),
                ('15min', '15 Minutes', 15, 'Fifteen minutes interval'),
                ('1h', '1 Hour', 60, 'One hour interval'),
                ('4h', '4 Hours', 240, 'Four hours interval'),
                ('1day', '1 Day', 1440, 'Daily interval')
            ON CONFLICT (interval_code) DO NOTHING;
        """)

        // Dimension table for exchanges
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS dim_exchange (
                exchange_id SERIAL PRIMARY KEY,
                exchange_code VARCHAR(20) NOT NULL UNIQUE,
                exchange_name VARCHAR(100) NOT NULL,
                country VARCHAR(50),
                timezone VARCHAR(50),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Table 'dim_exchange' created.")

        logger.info("Dimension tables created successfully.")
    }

    private fun createFactTables() {
        logger.info("Creating fact tables...")

        // Unified fact table for OHLC data
        jdbcTemplate.execute("""
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
        """)
        logger.info("Table 'fact_ohlc_data' created.")

        // Convert to hypertable for TimescaleDB
        try {
            jdbcTemplate.execute("""
                SELECT create_hypertable('fact_ohlc_data', 'timestamp',
                    if_not_exists => TRUE,
                    chunk_time_interval => INTERVAL '1 day'
                );
            """)
            logger.info("Converted 'fact_ohlc_data' to TimescaleDB hypertable.")
        } catch (e: Exception) {
            logger.warn("Could not convert fact_ohlc_data to hypertable: ${e.message}")
        }

        logger.info("Fact tables created successfully.")
    }

    private fun createMaterializedViews() {
        logger.info("Creating materialized views...")

        // Materialized view for daily aggregates across all asset types
        jdbcTemplate.execute("""
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
        """)
        logger.info("Materialized view 'mv_daily_ohlc_summary' created.")

        // Materialized view for hourly aggregates
        jdbcTemplate.execute("""
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
        """)
        logger.info("Materialized view 'mv_hourly_ohlc_summary' created.")

        // Materialized view for asset type summary
        jdbcTemplate.execute("""
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
        """)
        logger.info("Materialized view 'mv_asset_type_summary' created.")

        // Materialized view for symbol-specific analytics
        jdbcTemplate.execute("""
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
        """)
        logger.info("Materialized view 'mv_symbol_analytics' created.")

        logger.info("Materialized views created successfully.")
    }

    private fun createDataWarehouseIndexes() {
        logger.info("Creating data warehouse indexes...")

        // Indexes on dimension tables
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_dim_symbol_asset_type ON dim_symbol(asset_type_id);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_dim_symbol_code ON dim_symbol(symbol_code);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_dim_symbol_active ON dim_symbol(is_active) WHERE is_active = true;")

        // Indexes on fact table for time-range queries
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_fact_ohlc_timestamp ON fact_ohlc_data(timestamp DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_fact_ohlc_symbol_timestamp ON fact_ohlc_data(symbol_id, timestamp DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_fact_ohlc_asset_type_timestamp ON fact_ohlc_data(asset_type_id, timestamp DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_fact_ohlc_interval_timestamp ON fact_ohlc_data(interval_id, timestamp DESC);")
        
        // Composite indexes for common query patterns
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_fact_ohlc_symbol_interval_timestamp ON fact_ohlc_data(symbol_id, interval_id, timestamp DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_fact_ohlc_asset_symbol_timestamp ON fact_ohlc_data(asset_type_id, symbol_id, timestamp DESC);")

        // Indexes on materialized views
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mv_daily_symbol_day ON mv_daily_ohlc_summary(symbol_code, trading_day DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mv_daily_asset_day ON mv_daily_ohlc_summary(asset_type_code, trading_day DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mv_hourly_symbol_hour ON mv_hourly_ohlc_summary(symbol_code, trading_hour DESC);")
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mv_symbol_analytics_symbol ON mv_symbol_analytics(symbol_code);")

        logger.info("Data warehouse indexes created successfully.")
    }

    private fun setupContinuousAggregates() {
        logger.info("Setting up continuous aggregates with TimescaleDB...")

        try {
            // Create continuous aggregate for daily data
            jdbcTemplate.execute("""
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
            """)
            logger.info("Continuous aggregate 'cagg_daily_ohlc' created.")

            // Add refresh policy for daily aggregate (refresh every hour)
            jdbcTemplate.execute("""
                SELECT add_continuous_aggregate_policy('cagg_daily_ohlc',
                    start_offset => INTERVAL '3 days',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour',
                    if_not_exists => TRUE
                );
            """)
            logger.info("Refresh policy added for 'cagg_daily_ohlc'.")

            // Create continuous aggregate for hourly data
            jdbcTemplate.execute("""
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
            """)
            logger.info("Continuous aggregate 'cagg_hourly_ohlc' created.")

            // Add refresh policy for hourly aggregate (refresh every 15 minutes)
            jdbcTemplate.execute("""
                SELECT add_continuous_aggregate_policy('cagg_hourly_ohlc',
                    start_offset => INTERVAL '1 day',
                    end_offset => INTERVAL '15 minutes',
                    schedule_interval => INTERVAL '15 minutes',
                    if_not_exists => TRUE
                );
            """)
            logger.info("Refresh policy added for 'cagg_hourly_ohlc'.")

        } catch (e: Exception) {
            logger.warn("Could not create continuous aggregates: ${e.message}")
            logger.info("This is expected if TimescaleDB extension is not fully enabled.")
        }

        logger.info("Continuous aggregates setup completed.")
    }
}
