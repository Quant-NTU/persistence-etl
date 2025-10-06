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
     * Optimized to avoid large table joins by using streaming approach.
     */
    fun transformData() {
        logger.info("Starting optimized stock data transformation...")

        try {
            // Get distinct symbols and intervals to process
            val symbolsAndIntervals = jdbcTemplate.queryForList("""
                SELECT DISTINCT symbol, interval 
                FROM raw_stock_data 
                WHERE NOT EXISTS (
                    SELECT 1 FROM transformed_stock_data t 
                    WHERE t.symbol = raw_stock_data.symbol 
                    AND t.interval = raw_stock_data.interval
                    AND t.timestamp = raw_stock_data.timestamp
                )
                ORDER BY symbol, interval
            """)

            var totalRowsProcessed = 0
            
            for (row in symbolsAndIntervals) {
                val symbol = row["symbol"] as String
                val interval = row["interval"] as String
                
                val rowsProcessed = transformDataForSymbolAndInterval(symbol, interval)
                totalRowsProcessed += rowsProcessed
                
                logger.info("Processed $rowsProcessed records for $symbol ($interval)")
            }
            
            logger.info("Stock transformation completed successfully. Total rows processed: $totalRowsProcessed")
        } catch (e: Exception) {
            logger.error("Error during stock transformation: ${e.message}")
        }
    }

    private fun transformDataForSymbolAndInterval(symbol: String, interval: String): Int {
        // Use INSERT ... ON CONFLICT to avoid duplicates without expensive joins
        val query = """
            INSERT INTO transformed_stock_data (
                symbol, 
                interval,
                open, 
                high, 
                low, 
                close, 
                volume, 
                price_change, 
                timestamp
            )
            SELECT 
                symbol,
                interval,
                open,
                high,
                low,
                close,
                volume,
                CASE 
                    WHEN open > 0 THEN ((close - open) / open) * 100 
                    ELSE 0 
                END AS price_change,
                timestamp
            FROM raw_stock_data 
            WHERE symbol = ? AND interval = ?
            ON CONFLICT (symbol, interval, timestamp) DO NOTHING
        """

        return jdbcTemplate.update(query, symbol, interval)
    }
}
