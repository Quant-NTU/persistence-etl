package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

@Service
class CryptoTransformationService(private val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoTransformationService::class.java)

    /**
     * Transform raw data and save it into the transformed table.
     */
    fun transformData() {
        logger.info("Starting data transformation...")

        val query = """
            INSERT INTO transformed_crypto_data (symbol, currency, open, high, low, close, volume_from, volume_to, avg_price, price_change, timestamp, "source")
            SELECT 
                symbol,
                currency,
                MIN(open) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                MAX(close) AS close,
                SUM(volume_from) AS volume_from,
                SUM(volume_to) AS volume_to,
                AVG((high + low) / 2) AS avg_price,
                ((MAX(close) - MIN(open)) / MIN(open)) * 100 AS price_change,
                date_trunc('day', timestamp) AS timestamp,
                source
            FROM raw_crypto_data
            WHERE timestamp > (SELECT COALESCE(MAX(timestamp), '1970-01-01') FROM transformed_crypto_data)
            GROUP BY symbol, currency, date_trunc('day', timestamp), source;
        """

        try {
            val rowsAffected = jdbcTemplate.update(query)
            logger.info("Transformation completed successfully. Rows inserted: $rowsAffected")
        } catch (e: Exception) {
            logger.error("Error during transformation: ${e.message}")
        }
    }
}