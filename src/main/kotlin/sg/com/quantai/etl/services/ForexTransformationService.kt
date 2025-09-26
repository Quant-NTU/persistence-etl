package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

@Service
class ForexTransformationService(private val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(ForexTransformationService::class.java)

    /**
     * Transform raw data from raw_forex_data and save it into transformed_forex_data.
     */
    fun transformData() {
        logger.info("Starting forex data transformation...")

        val query = """
            INSERT INTO transformed_forex_data (
                currency_pair, 
                open, 
                high, 
                low, 
                close, 
                avg_price, 
                price_change, 
                timestamp
            )
            SELECT 
                raw.currency_pair,
                MIN(raw.open) AS open,
                MAX(raw.high) AS high,
                MIN(raw.low) AS low,
                MAX(raw.close) AS close,
                AVG((raw.high + raw.low) / 2) AS avg_price,
                ((MAX(raw.close) - MIN(raw.open)) / MIN(raw.open)) * 100 AS price_change,
                date_trunc('day', raw.timestamp) AS timestamp
            FROM raw_forex_data raw
            LEFT JOIN transformed_forex_data transformed
            ON raw.currency_pair = transformed.currency_pair
            AND date_trunc('day', raw.timestamp) = transformed.timestamp
            WHERE transformed.timestamp IS NULL
            GROUP BY raw.currency_pair, date_trunc('day', raw.timestamp);
        """

        try {
            val rowsAffected = jdbcTemplate.update(query)
            logger.info("Forex transformation completed successfully. Rows inserted: $rowsAffected")
        } catch (e: Exception) {
            logger.error("Error during forex transformation: ${e.message}")
        }
    }
}
