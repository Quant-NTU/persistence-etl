package sg.com.quantai.etl.services.stock

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicBoolean

@Service
class StockTransformService(
    private val jdbcTemplate: JdbcTemplate
) {
    private val logger: Logger = LoggerFactory.getLogger(StockTransformService::class.java)
    private val isProcessing = AtomicBoolean(false)

    fun transform() {
        if (isProcessing.getAndSet(true)) {
            logger.warn("Stock data transformation is already in progress")
            return
        }

        try {
            logger.info("Starting stock data transformation")
            truncateTransformedData()
            handleMissingValues()
            val transformationQuery = """
                INSERT INTO transformed_stock_data (
                    ticker, 
                    date, 
                    open, 
                    high, 
                    low, 
                    close, 
                    volume, 
                    closeadj, 
                    price_change, 
                    volatility, 
                    vwap
                )
                WITH prepared_data AS (
                    SELECT 
                        ticker,
                        date,
                        COALESCE(
                            open,
                            LAG(close) OVER (PARTITION BY ticker ORDER BY date),
                            (high + low) / 2,
                            close
                        ) AS open,
                        COALESCE(high, GREATEST(COALESCE(open, 0), COALESCE(close, 0))) AS high,
                        COALESCE(low, LEAST(COALESCE(open, 999999), COALESCE(close, 999999))) AS low,
                        COALESCE(close, open) AS close,
                        COALESCE(
                            NULLIF(volume, 0),
                            AVG(NULLIF(volume, 0)) OVER (
                                PARTITION BY ticker 
                                ORDER BY date 
                                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                            ),
                            1
                        ) AS volume,
                        closeadj
                    FROM raw_stock_nasdaq_data
                )
                SELECT 
                    p.ticker,
                    p.date,
                    p.open,
                    p.high,
                    p.low,
                    p.close,
                    p.volume,
                    p.closeadj,
                    CASE 
                        WHEN p.open = 0 OR p.open IS NULL THEN 0
                        ELSE ((p.close - p.open) / p.open) * 100 
                    END AS price_change,
                    CASE 
                        WHEN p.open = 0 OR p.open IS NULL THEN 0
                        ELSE ((p.high - p.low) / p.open) * 100 
                    END AS volatility,
                    CASE
                        WHEN SUM(p.volume) OVER (PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) = 0 THEN p.close
                        ELSE (SUM(p.close * p.volume) OVER (PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) / 
                             SUM(p.volume) OVER (PARTITION BY p.ticker ORDER BY p.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                    END AS vwap -- Volume Weighted Average Price (VWAP)
                FROM prepared_data p
            """
            val rowsAffected = jdbcTemplate.update(transformationQuery)
            logger.info("Initial transformation completed. Rows affected: $rowsAffected")
            updateSMA7()
            logger.info("Stock data transformation completed successfully")
        } catch (e: Exception) {
            logger.error("Error during stock data transformation", e)
            e.printStackTrace()
        } finally {
            isProcessing.set(false)
        }
    }

    private fun truncateTransformedData() {
        try {
            logger.info("Truncating transformed_stock_data table")
            val rowsDeleted = jdbcTemplate.update("DELETE FROM transformed_stock_data")
            logger.info("Deleted $rowsDeleted existing rows from transformed_stock_data table")
        } catch (e: Exception) {
            logger.error("Error truncating transformed_stock_data table: ${e.message}")
        }
    }

    private fun updateSMA7() {
        try {
            val smaUpdateQuery = """
                UPDATE transformed_stock_data t
                SET sma_7 = avg_data.sma
                FROM (
                    SELECT 
                        ticker,
                        date,
                        AVG(close) OVER (
                            PARTITION BY ticker 
                            ORDER BY date 
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                        ) AS sma
                    FROM transformed_stock_data
                ) AS avg_data
                WHERE t.ticker = avg_data.ticker AND t.date = avg_data.date
            """

            val smaRowsAffected = jdbcTemplate.update(smaUpdateQuery)
            logger.info("SMA_7 update completed. Rows affected: $smaRowsAffected")
        } catch (e: Exception) {
            logger.error("Error updating SMA_7 values: ${e.message}")
        }
    }

    private fun handleMissingValues() {
        try {
            jdbcTemplate.execute("""
                WITH close_values AS (
                    SELECT 
                        r.ticker,
                        r.date,
                        r.close,
                        (
                            SELECT c.close 
                            FROM raw_stock_nasdaq_data c 
                            WHERE c.ticker = r.ticker 
                            AND c.date <= r.date 
                            AND c.close IS NOT NULL 
                            ORDER BY c.date DESC 
                            LIMIT 1
                        ) AS filled_close
                    FROM raw_stock_nasdaq_data r
                    WHERE r.close IS NULL
                )
                UPDATE raw_stock_nasdaq_data r
                SET close = cv.filled_close
                FROM close_values cv
                WHERE r.ticker = cv.ticker 
                AND r.date = cv.date 
                AND r.close IS NULL 
                AND cv.filled_close IS NOT NULL
            """)

            jdbcTemplate.execute("""
                UPDATE raw_stock_nasdaq_data
                SET 
                    high = GREATEST(COALESCE(high, 0), COALESCE(open, 0), COALESCE(close, 0)),
                    low = CASE 
                        WHEN low IS NULL THEN LEAST(COALESCE(open, 999999), COALESCE(close, 999999))
                        ELSE low
                    END
                WHERE (high IS NULL OR low IS NULL)
                AND (open IS NOT NULL OR close IS NOT NULL)
            """)

            logger.info("Missing values handling completed")
        } catch (e: Exception) {
            logger.error("Error handling missing values", e)
        }
    }
}