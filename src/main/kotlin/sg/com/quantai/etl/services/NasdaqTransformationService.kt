package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.scheduling.annotation.Scheduled

/**
 * Service class responsible for transforming raw NASDAQ stock market data into a more usable format.
 * This service handles the ETL (Extract, Transform, Load) process for NASDAQ data.
 */
@Service
class NasdaqTransformationService(private val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(NasdaqTransformationService::class.java)

    init {
        createTransformedTable()
    }

    /**
     * Creates the transformed data table if it doesn't exist.
     * This table stores processed NASDAQ data with additional calculated fields.
     * Fields include:
     * - symbol: Stock symbol
     * - date_time: Timestamp of the data point
     * - open/high/low/close: Price data points
     * - volume: Trading volume
     * - avg_price: Calculated average price ((high + low) / 2)
     * - price_change: Percentage change in price ((close - open) / open * 100)
     */
    private fun createTransformedTable() {
        val sql = """
            CREATE TABLE IF NOT EXISTS transformed_nasdaq_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date_time TIMESTAMP WITH TIME ZONE NOT NULL,
                open DECIMAL NOT NULL,
                high DECIMAL NOT NULL,
                low DECIMAL NOT NULL,
                close DECIMAL NOT NULL,
                volume DECIMAL NOT NULL,
                avg_price DECIMAL,
                price_change DECIMAL
            )
        """
        jdbcTemplate.execute(sql)
    }

    /**
     * Scheduled job that transforms raw NASDAQ data into processed format.
     * Runs daily at 1:30 AM UTC.
     * 
     * The transformation process:
     * 1. Selects data from raw_nasdaq_data table
     * 2. Calculates average price and price change percentage
     * 3. Inserts only new records (not already in transformed table)
     * 4. Logs the number of rows transformed
     */
    @Scheduled(cron = "0 30 1 * * ?") // Runs daily at 1:30 AM UTC
    fun transformData() {
        logger.info("Starting NASDAQ data transformation...")
        
        val sql = """
            INSERT INTO transformed_nasdaq_data (
                symbol, date_time, open, high, low, close, volume, avg_price, price_change
            )
            SELECT 
                symbol,
                date_time,
                open,
                high,
                low,
                close,
                volume,
                (high + low) / 2 as avg_price,
                ((close - open) / open) * 100 as price_change
            FROM raw_nasdaq_data raw
            WHERE NOT EXISTS (
                SELECT 1 FROM transformed_nasdaq_data t
                WHERE t.symbol = raw.symbol 
                AND t.date_time = raw.date_time
            )
        """

        try {
            val rowsAffected = jdbcTemplate.update(sql)
            logger.info("Transformation completed. $rowsAffected rows transformed.")
        } catch (e: Exception) {
            logger.error("Error during transformation: ${e.message}")
        }
    }
} 