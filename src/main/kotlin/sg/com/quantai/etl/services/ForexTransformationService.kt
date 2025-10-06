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
     * Optimized to avoid large table joins by using streaming approach.
     */
    fun transformData() {
        logger.info("Starting optimized forex data transformation...")

        try {
            // Get distinct currency pairs and intervals to process
            val pairsAndIntervals = jdbcTemplate.queryForList("""
                SELECT DISTINCT currency_pair, interval 
                FROM raw_forex_data 
                WHERE NOT EXISTS (
                    SELECT 1 FROM transformed_forex_data t 
                    WHERE t.currency_pair = raw_forex_data.currency_pair 
                    AND t.interval = raw_forex_data.interval
                    AND t.timestamp = raw_forex_data.timestamp
                )
                ORDER BY currency_pair, interval
            """)

            var totalRowsProcessed = 0
            
            for (row in pairsAndIntervals) {
                val currencyPair = row["currency_pair"] as String
                val interval = row["interval"] as String
                
                val rowsProcessed = transformDataForPairAndInterval(currencyPair, interval)
                totalRowsProcessed += rowsProcessed
                
                logger.info("Processed $rowsProcessed records for $currencyPair ($interval)")
            }
            
            logger.info("Forex transformation completed successfully. Total rows processed: $totalRowsProcessed")
        } catch (e: Exception) {
            logger.error("Error during forex transformation: ${e.message}")
        }
    }

    private fun transformDataForPairAndInterval(currencyPair: String, interval: String): Int {
        // Use INSERT ... ON CONFLICT to avoid duplicates without expensive joins
        val query = """
            INSERT INTO transformed_forex_data (
                currency_pair, 
                interval,
                open, 
                high, 
                low, 
                close, 
                price_change, 
                timestamp
            )
            SELECT 
                currency_pair,
                interval,
                open,
                high,
                low,
                close,
                CASE 
                    WHEN open > 0 THEN ((close - open) / open) * 100 
                    ELSE 0 
                END AS price_change,
                timestamp
            FROM raw_forex_data 
            WHERE currency_pair = ? AND interval = ?
            ON CONFLICT (currency_pair, interval, timestamp) DO NOTHING
        """

        return jdbcTemplate.update(query, currencyPair, interval)
    }
}
