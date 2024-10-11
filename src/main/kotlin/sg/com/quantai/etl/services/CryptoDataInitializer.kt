package sg.com.quantai.etl.services

import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class CryptoDataInitializer(val jdbcTemplate: JdbcTemplate) {

    @Bean
    fun runDatabaseInitializer(): CommandLineRunner {
        return CommandLineRunner {
            // Create Raw Crypto Data Table
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS raw_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    price DECIMAL NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            println("Table 'raw_crypto_data' initialized.")

            // Create Transformed Crypto Data Table
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS transformed_crypto_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    average_price DECIMAL NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
            """)
            println("Table 'transformed_crypto_data' initialized.")
        }
    }
}
