package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

/**
 * Initializes required tables in TimescaleDB on application startup.
 *
 * This component defines the database schema for tables in TimescaleDB, 
 * specifically `raw_crypto_data` and `transformed_crypto_data`, 
 * which store raw and aggregated cryptocurrency data respectively.
 *
 * @property jdbcTemplate JdbcTemplate instance for executing SQL statements.
 */
@Component
class TimescaleDBTableInitializer(val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(TimescaleDBTableInitializer::class.java)

    /**
     * Returns a CommandLineRunner bean to initialize tables on startup.
     *
     * The `runDatabaseInitializer` method is annotated with `@Bean`, ensuring it runs at startup. 
     * This method creates the `raw_crypto_data` and `transformed_crypto_data` tables if they do not exist, 
     * logging the initialization process.
     *
     * @return CommandLineRunner A Spring Boot command-line runner that executes the database initialization process.
     */
    @Bean(name = ["timescaleDBInitializer"])
    fun runDatabaseInitializer(): CommandLineRunner {
        return CommandLineRunner {

            // Start of table initialization
            logger.info("Initializing all required tables for TimescaleDB...")

            // Initialize `raw_crypto_data` table
            logger.info("Initializing 'raw_crypto_data' table...")
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS raw_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    currency VARCHAR(10) NOT NULL,
                    open DECIMAL NOT NULL,
                    high DECIMAL NOT NULL,
                    low DECIMAL NOT NULL,
                    close DECIMAL NOT NULL,
                    volume_from DECIMAL NOT NULL,
                    volume_to DECIMAL NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    "source" VARCHAR(50) NOT NULL
                );
            """)
            logger.info("Table 'raw_crypto_data' initialized.")

            // Initialize `transformed_crypto_data` table
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
                    timestamp TIMESTAMPTZ NOT NULL,
                    "source" VARCHAR(50) NOT NULL
                );
            """)
            logger.info("Table 'transformed_crypto_data' initialized.")

            // End of table initialization
            logger.info("All tables have been successfully initialized.")
        
        }
    }
}
