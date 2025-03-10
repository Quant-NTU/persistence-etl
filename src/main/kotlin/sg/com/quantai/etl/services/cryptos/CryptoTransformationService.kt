package sg.com.quantai.etl.services.cryptos

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

@Service
class CryptoTransformationService(private val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoTransformationService::class.java)

    /**
     * Transform raw data from raw_crypto_compare_crypto_data and save it into transformed_crypto_data.
     */
    fun transformData() {
        logger.info("Starting data transformation...")

        val query = """
            INSERT INTO transformed_crypto_data (
                symbol, 
                currency, 
                open, 
                high, 
                low, 
                close, 
                volume_from, 
                volume_to, 
                avg_price, 
                price_change, 
                timestamp
            )
            SELECT 
                raw.symbol,
                raw.currency,
                MIN(raw.open) AS open,
                MAX(raw.high) AS high,
                MIN(raw.low) AS low,
                MAX(raw.close) AS close,
                SUM(raw.volume_from) AS volume_from,
                SUM(raw.volume_to) AS volume_to,
                AVG((raw.high + raw.low) / 2) AS avg_price,
                ((MAX(raw.close) - MIN(raw.open)) / MIN(raw.open)) * 100 AS price_change,
                date_trunc('day', raw.timestamp) AS timestamp
            FROM raw_crypto_compare_crypto_data raw
            LEFT JOIN transformed_crypto_data transformed
            ON raw.symbol = transformed.symbol
            AND raw.currency = transformed.currency
            AND date_trunc('day', raw.timestamp) = transformed.timestamp
            WHERE transformed.timestamp IS NULL
            GROUP BY raw.symbol, raw.currency, date_trunc('day', raw.timestamp);
        """

        try {
            val rowsAffected = jdbcTemplate.update(query)
            logger.info("Transformation completed successfully. Rows inserted: $rowsAffected")
        } catch (e: Exception) {
            logger.error("Error during transformation: ${e.message}")
        }
    }
}