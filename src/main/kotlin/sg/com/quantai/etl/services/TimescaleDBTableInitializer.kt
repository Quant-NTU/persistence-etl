package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

/**
 * Component responsible for initializing TimescaleDB tables required for the application.
 * This initializer runs on application startup and creates necessary database tables
 * if they don't already exist.
 */
@Component
class TimescaleDBTableInitializer(val jdbcTemplate: JdbcTemplate) {

    // Logger instance for this class
    private val logger: Logger = LoggerFactory.getLogger(TimescaleDBTableInitializer::class.java)

    /**
     * Creates a CommandLineRunner bean that initializes database tables.
     * This runs automatically when the application starts.
     * 
     * Creates two main tables:
     * 1. raw_crypto_compare_crypto_data - Stores raw cryptocurrency data from CryptoCompare API
     * 2. transformed_crypto_data - Stores processed cryptocurrency data with additional metrics
     * 
     * @return CommandLineRunner that executes table creation
     */
    @Bean(name = ["timescaleDBInitializer"])
    fun runDatabaseInitializer(): CommandLineRunner {
        return CommandLineRunner {

            logger.info("Initializing all required tables for TimescaleDB...")

            // Create raw data table for storing cryptocurrency data directly from CryptoCompare
            logger.info("Initializing 'raw_crypto_compare_crypto_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS raw_crypto_compare_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,          -- Cryptocurrency symbol (e.g., BTC)
                    currency VARCHAR(10) NOT NULL,        -- Base currency (e.g., USD)
                    open DECIMAL NOT NULL,               -- Opening price for the period
                    high DECIMAL NOT NULL,               -- Highest price during the period
                    low DECIMAL NOT NULL,                -- Lowest price during the period
                    close DECIMAL NOT NULL,              -- Closing price for the period
                    volume_from DECIMAL NOT NULL,        -- Volume in cryptocurrency
                    volume_to DECIMAL NOT NULL,          -- Volume in base currency
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL  -- Time of the data point
                );
            """)
            logger.info("Table 'raw_crypto_compare_crypto_data' initialized.")

            // Create transformed data table for storing processed cryptocurrency data
            logger.info("Initializing 'transformed_crypto_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS transformed_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,          -- Cryptocurrency symbol
                    currency VARCHAR(10) NOT NULL,        -- Base currency
                    open DECIMAL NOT NULL,               -- Opening price
                    high DECIMAL NOT NULL,               -- Highest price
                    low DECIMAL NOT NULL,                -- Lowest price
                    close DECIMAL NOT NULL,              -- Closing price
                    volume_from DECIMAL NOT NULL,        -- Volume in cryptocurrency
                    volume_to DECIMAL NOT NULL,          -- Volume in base currency
                    avg_price DECIMAL,                   -- Average price ((high + low) / 2)
                    price_change DECIMAL,                -- Price change percentage ((close - open) / open * 100)
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL  -- Time of the data point
                );
            """)
            logger.info("Table 'transformed_crypto_data' initialized.")

            logger.info("All tables have been successfully initialized.")
        }
    }
}