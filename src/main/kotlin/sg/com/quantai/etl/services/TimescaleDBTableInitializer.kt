package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class TimescaleDBTableInitializer(val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(TimescaleDBTableInitializer::class.java)

    @Bean(name = ["timescaleDBInitializer"])
    fun runDatabaseInitializer(): CommandLineRunner {
        return CommandLineRunner {

            logger.info("Initializing all required tables for TimescaleDB...")

            logger.info("Initializing 'raw_crypto_compare_crypto_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS raw_crypto_compare_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    currency VARCHAR(10) NOT NULL,
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    volume_from DECIMAL NOT NULL,
                    volume_to DECIMAL NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            logger.info("Table 'raw_crypto_compare_crypto_data' initialized.")

            logger.info("Initializing 'transformed_crypto_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS transformed_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    currency VARCHAR(10) NOT NULL,
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    volume_from DECIMAL NOT NULL,
                    volume_to DECIMAL NOT NULL,
                    avg_price DECIMAL,
                    price_change DECIMAL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            logger.info("Table 'transformed_crypto_data' initialized.")

            logger.info("Initializing 'raw_stock_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS raw_stock_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    interval VARCHAR(10) NOT NULL DEFAULT '1day',
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    volume BIGINT NOT NULL,
                    start_date_time TIMESTAMPTZ,
                    end_date_time TIMESTAMPTZ,
                    timestamp TIMESTAMPTZ NOT NULL,
                    source_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    sys_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info("Table 'raw_stock_data' initialized.")

            logger.info("Initializing 'transformed_stock_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS transformed_stock_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    interval VARCHAR(10) NOT NULL DEFAULT '1day',
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    volume BIGINT NOT NULL,
                    price_change DECIMAL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            logger.info("Table 'transformed_stock_data' initialized.")

            logger.info("Initializing 'raw_forex_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS raw_forex_data (
                    id SERIAL PRIMARY KEY,
                    currency_pair VARCHAR(10) NOT NULL,
                    interval VARCHAR(10) NOT NULL DEFAULT '1day',
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    start_date_time TIMESTAMPTZ,
                    end_date_time TIMESTAMPTZ,
                    timestamp TIMESTAMPTZ NOT NULL,
                    source_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    sys_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info("Table 'raw_forex_data' initialized.")

            logger.info("Initializing 'transformed_forex_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS transformed_forex_data (
                    id SERIAL PRIMARY KEY,
                    currency_pair VARCHAR(10) NOT NULL,
                    interval VARCHAR(10) NOT NULL DEFAULT '1day',
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    price_change DECIMAL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            logger.info("Table 'transformed_forex_data' initialized.")

            // Create indexes for query performance optimization
            logger.info("Creating database indexes for performance optimization...")
            
            // Indexes for raw_stock_data
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_raw_stock_symbol_timestamp ON raw_stock_data (symbol, timestamp DESC);")
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_raw_stock_interval_timestamp ON raw_stock_data (interval, timestamp DESC);")
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_raw_stock_symbol_interval ON raw_stock_data (symbol, interval);")
            
            // Indexes for transformed_stock_data  
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_transformed_stock_symbol_timestamp ON transformed_stock_data (symbol, timestamp DESC);")
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_transformed_stock_interval_timestamp ON transformed_stock_data (interval, timestamp DESC);")
            
            // Indexes for raw_forex_data
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_raw_forex_pair_timestamp ON raw_forex_data (currency_pair, timestamp DESC);")
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_raw_forex_interval_timestamp ON raw_forex_data (interval, timestamp DESC);")
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_raw_forex_pair_interval ON raw_forex_data (currency_pair, interval);")
            
            // Indexes for transformed_forex_data
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_transformed_forex_pair_timestamp ON transformed_forex_data (currency_pair, timestamp DESC);")
            jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_transformed_forex_interval_timestamp ON transformed_forex_data (interval, timestamp DESC);")
            
            logger.info("Database indexes created successfully.")
            
            // Add unique constraints for duplicate prevention
            logger.info("Adding unique constraints for duplicate prevention...")
            
            try {
                jdbcTemplate.execute("ALTER TABLE raw_stock_data ADD CONSTRAINT uk_raw_stock_symbol_interval_timestamp UNIQUE (symbol, interval, timestamp);")
            } catch (e: Exception) {
                logger.info("Unique constraint for raw_stock_data already exists or failed to create: ${e.message}")
            }
            
            try {
                jdbcTemplate.execute("ALTER TABLE raw_forex_data ADD CONSTRAINT uk_raw_forex_pair_interval_timestamp UNIQUE (currency_pair, interval, timestamp);")
            } catch (e: Exception) {
                logger.info("Unique constraint for raw_forex_data already exists or failed to create: ${e.message}")
            }
            
            try {
                jdbcTemplate.execute("ALTER TABLE transformed_stock_data ADD CONSTRAINT uk_transformed_stock_symbol_interval_timestamp UNIQUE (symbol, interval, timestamp);")
            } catch (e: Exception) {
                logger.info("Unique constraint for transformed_stock_data already exists or failed to create: ${e.message}")
            }
            
            try {
                jdbcTemplate.execute("ALTER TABLE transformed_forex_data ADD CONSTRAINT uk_transformed_forex_pair_interval_timestamp UNIQUE (currency_pair, interval, timestamp);")
            } catch (e: Exception) {
                logger.info("Unique constraint for transformed_forex_data already exists or failed to create: ${e.message}")
            }
            
            logger.info("Unique constraints added successfully.")

            logger.info("All tables, indexes, and constraints have been successfully initialized.")
        }
    }
}