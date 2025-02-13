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

            logger.info("All tables have been successfully initialized.")
        }
    }
}