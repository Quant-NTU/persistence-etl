package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

@Service
class StockTransformationService(private val jdbcTemplate: JdbcTemplate) {

    private val logger: Logger = LoggerFactory.getLogger(StockTransformationService::class.java)

    /**
     * Transform raw data from raw_stock_data and save it into transformed_stock_data.
     */
    fun transformData() {
        logger.info("Starting stock data transformation...")

        val query = """
            INSERT INTO transformed_stock_data (
                symbol, 
                open, 
                high, 
                low, 
                close, 
                volume, 
                avg_price, 
                price_change, 
                timestamp
            )
            SELECT 
                raw.symbol,
                MIN(raw.open) AS open,
                MAX(raw.high) AS high,
                MIN(raw.low) AS low,
                MAX(raw.close) AS close,
                SUM(raw.volume) AS volume,
                AVG((raw.high + raw.low) / 2) AS avg_price,
                ((MAX(raw.close) - MIN(raw.open)) / MIN(raw.open)) * 100 AS price_change,
                date_trunc('day', raw.timestamp) AS timestamp
            FROM raw_stock_data raw
            LEFT JOIN transformed_stock_data transformed
            ON raw.symbol = transformed.symbol
            AND date_trunc('day', raw.timestamp) = transformed.timestamp
            WHERE transformed.timestamp IS NULL
            GROUP BY raw.symbol, date_trunc('day', raw.timestamp);
        """

        try {
            val rowsAffected = jdbcTemplate.update(query)
            logger.info("Stock transformation completed successfully. Rows inserted: $rowsAffected")
        } catch (e: Exception) {
            logger.error("Error during stock transformation: ${e.message}")
        }
    }
}
