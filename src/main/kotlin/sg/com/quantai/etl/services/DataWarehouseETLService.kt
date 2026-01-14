package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Service
class DataWarehouseETLService(
    private val jdbcTemplate: JdbcTemplate
) {

    private val logger: Logger = LoggerFactory.getLogger(DataWarehouseETLService::class.java)
    
    companion object {
        val VALID_ASSET_TYPES = listOf("STOCK", "FOREX", "CRYPTO")
        val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    }

    /**
     * Load all data from source tables into the data warehouse
     * This should be run initially to populate the warehouse
     * 
     * @param assetTypes List of asset types to load (null = all)
     * @return Map containing load statistics
     */
    @Transactional
    fun loadAllDataToWarehouse(assetTypes: List<String>? = null): Map<String, Any> {
        logger.info("Starting full data warehouse load...")
        val startTime = LocalDateTime.now()
        val stats = mutableMapOf<String, Any>()
        val recordCounts = mutableMapOf<String, Long>()
        
        val typesToLoad = if (assetTypes.isNullOrEmpty()) {
            VALID_ASSET_TYPES
        } else {
            assetTypes.filter { it in VALID_ASSET_TYPES }
        }
        
        if (typesToLoad.contains("STOCK")) {
            recordCounts["stock"] = loadStockData()
        }
        if (typesToLoad.contains("FOREX")) {
            recordCounts["forex"] = loadForexData()
        }
        if (typesToLoad.contains("CRYPTO")) {
            recordCounts["crypto"] = loadCryptoData()
        }
        
        val viewStats = refreshMaterializedViewsInternal()
        
        val endTime = LocalDateTime.now()
        val duration = java.time.Duration.between(startTime, endTime)
        
        stats["loadType"] = "full"
        stats["assetTypesLoaded"] = typesToLoad
        stats["recordCounts"] = recordCounts
        stats["totalRecordsLoaded"] = recordCounts.values.sum()
        stats["viewsRefreshed"] = viewStats
        stats["startTime"] = startTime.format(DATE_FORMATTER)
        stats["endTime"] = endTime.format(DATE_FORMATTER)
        stats["durationSeconds"] = duration.seconds
        stats["dateRange"] = getDataDateRange()
        
        logger.info("Full data warehouse load completed. Stats: $stats")
        return stats
    }

    /**
     * Scheduled incremental load - runs every hour with default parameters
     */
    @Scheduled(cron = "0 0 * * * *") // Every hour at minute 0
    fun scheduledIncrementalLoad() {
        incrementalLoadToWarehouse(null, 2)
    }

    /**
     * Incremental load - can be called manually with custom parameters
     * Loads only recent data to keep warehouse up to date
     * 
     * @param assetTypes List of asset types to load (null = all)
     * @param hoursBack Number of hours to look back (default: 2)
     * @return Map containing load statistics
     */
    @Transactional
    fun incrementalLoadToWarehouse(assetTypes: List<String>? = null, hoursBack: Int = 2): Map<String, Any> {
        logger.info("Starting incremental data warehouse load (${hoursBack} hours back)...")
        val startTime = LocalDateTime.now()
        val stats = mutableMapOf<String, Any>()
        val recordCounts = mutableMapOf<String, Long>()
        
        val typesToLoad = if (assetTypes.isNullOrEmpty()) {
            VALID_ASSET_TYPES
        } else {
            assetTypes.filter { it in VALID_ASSET_TYPES }
        }
        
        val cutoffTime = LocalDateTime.now().minusHours(hoursBack.toLong())
        
        if (typesToLoad.contains("STOCK")) {
            recordCounts["stock"] = loadStockDataIncremental(hoursBack)
        }
        if (typesToLoad.contains("FOREX")) {
            recordCounts["forex"] = loadForexDataIncremental(hoursBack)
        }
        if (typesToLoad.contains("CRYPTO")) {
            recordCounts["crypto"] = loadCryptoDataIncremental(hoursBack)
        }
        
        val viewStats = refreshMaterializedViewsInternal()
        
        val endTime = LocalDateTime.now()
        val duration = java.time.Duration.between(startTime, endTime)
        
        stats["loadType"] = "incremental"
        stats["assetTypesLoaded"] = typesToLoad
        stats["recordCounts"] = recordCounts
        stats["totalRecordsLoaded"] = recordCounts.values.sum()
        stats["viewsRefreshed"] = viewStats
        stats["hoursBack"] = hoursBack
        stats["dataFromTime"] = cutoffTime.format(DATE_FORMATTER)
        stats["startTime"] = startTime.format(DATE_FORMATTER)
        stats["endTime"] = endTime.format(DATE_FORMATTER)
        stats["durationSeconds"] = duration.seconds
        
        logger.info("Incremental data warehouse load completed. Stats: $stats")
        return stats
    }

    private fun loadStockData(): Long {
        logger.info("Loading stock data into data warehouse...")
        
        val countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')",
            Long::class.java
        ) ?: 0L
        
        // Ensure symbols exist in dimension table
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, is_active)
            SELECT DISTINCT 
                ts.symbol,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK'),
                ts.symbol,
                true
            FROM transformed_stock_data ts
            ON CONFLICT (symbol_code, asset_type_id) DO NOTHING;
        """)
        
        // Load stock OHLC data into fact table
        jdbcTemplate.execute("""
            INSERT INTO fact_ohlc_data (
                symbol_id, asset_type_id, interval_id, timestamp,
                open, high, low, close, volume, price_change, source_table
            )
            SELECT 
                ds.symbol_id,
                ds.asset_type_id,
                dti.interval_id,
                ts.timestamp,
                ts.open,
                ts.high,
                ts.low,
                ts.close,
                ts.volume,
                ts.price_change,
                'transformed_stock_data'
            FROM transformed_stock_data ts
            JOIN dim_symbol ds ON ts.symbol = ds.symbol_code 
                AND ds.asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')
            JOIN dim_time_interval dti ON ts.interval = dti.interval_code
            ON CONFLICT DO NOTHING;
        """)
        
        val countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')",
            Long::class.java
        ) ?: 0L
        
        val recordsLoaded = countAfter - countBefore
        logger.info("Loaded $recordsLoaded new stock records into data warehouse (total: $countAfter).")
        return recordsLoaded
    }

    private fun loadStockDataIncremental(hoursBack: Int = 2): Long {
        logger.info("Loading incremental stock data (last $hoursBack hours)...")
        
        val countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')",
            Long::class.java
        ) ?: 0L
        
        // Load only recent data
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, is_active)
            SELECT DISTINCT 
                ts.symbol,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK'),
                ts.symbol,
                true
            FROM transformed_stock_data ts
            WHERE ts.timestamp > NOW() - INTERVAL '$hoursBack hours'
            ON CONFLICT (symbol_code, asset_type_id) DO NOTHING;
        """)
        
        jdbcTemplate.execute("""
            INSERT INTO fact_ohlc_data (
                symbol_id, asset_type_id, interval_id, timestamp,
                open, high, low, close, volume, price_change, source_table
            )
            SELECT 
                ds.symbol_id,
                ds.asset_type_id,
                dti.interval_id,
                ts.timestamp,
                ts.open,
                ts.high,
                ts.low,
                ts.close,
                ts.volume,
                ts.price_change,
                'transformed_stock_data'
            FROM transformed_stock_data ts
            JOIN dim_symbol ds ON ts.symbol = ds.symbol_code 
                AND ds.asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')
            JOIN dim_time_interval dti ON ts.interval = dti.interval_code
            WHERE ts.timestamp > NOW() - INTERVAL '$hoursBack hours'
            ON CONFLICT DO NOTHING;
        """)
        
        val countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')",
            Long::class.java
        ) ?: 0L
        
        val recordsLoaded = countAfter - countBefore
        logger.info("Loaded $recordsLoaded new stock records (incremental).")
        return recordsLoaded
    }

    private fun loadForexData(): Long {
        logger.info("Loading forex data into data warehouse...")
        
        val countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')",
            Long::class.java
        ) ?: 0L
        
        // Ensure currency pairs exist in dimension table
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, is_active)
            SELECT DISTINCT 
                tf.currency_pair,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX'),
                tf.currency_pair,
                true
            FROM transformed_forex_data tf
            ON CONFLICT (symbol_code, asset_type_id) DO NOTHING;
        """)
        
        // Load forex OHLC data into fact table
        jdbcTemplate.execute("""
            INSERT INTO fact_ohlc_data (
                symbol_id, asset_type_id, interval_id, timestamp,
                open, high, low, close, price_change, source_table
            )
            SELECT 
                ds.symbol_id,
                ds.asset_type_id,
                dti.interval_id,
                tf.timestamp,
                tf.open,
                tf.high,
                tf.low,
                tf.close,
                tf.price_change,
                'transformed_forex_data'
            FROM transformed_forex_data tf
            JOIN dim_symbol ds ON tf.currency_pair = ds.symbol_code 
                AND ds.asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')
            JOIN dim_time_interval dti ON tf.interval = dti.interval_code
            ON CONFLICT DO NOTHING;
        """)
        
        val countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')",
            Long::class.java
        ) ?: 0L
        
        val recordsLoaded = countAfter - countBefore
        logger.info("Loaded $recordsLoaded new forex records into data warehouse (total: $countAfter).")
        return recordsLoaded
    }

    private fun loadForexDataIncremental(hoursBack: Int = 2): Long {
        logger.info("Loading incremental forex data (last $hoursBack hours)...")
        
        val countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')",
            Long::class.java
        ) ?: 0L
        
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, is_active)
            SELECT DISTINCT 
                tf.currency_pair,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX'),
                tf.currency_pair,
                true
            FROM transformed_forex_data tf
            WHERE tf.timestamp > NOW() - INTERVAL '$hoursBack hours'
            ON CONFLICT (symbol_code, asset_type_id) DO NOTHING;
        """)
        
        jdbcTemplate.execute("""
            INSERT INTO fact_ohlc_data (
                symbol_id, asset_type_id, interval_id, timestamp,
                open, high, low, close, price_change, source_table
            )
            SELECT 
                ds.symbol_id,
                ds.asset_type_id,
                dti.interval_id,
                tf.timestamp,
                tf.open,
                tf.high,
                tf.low,
                tf.close,
                tf.price_change,
                'transformed_forex_data'
            FROM transformed_forex_data tf
            JOIN dim_symbol ds ON tf.currency_pair = ds.symbol_code 
                AND ds.asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')
            JOIN dim_time_interval dti ON tf.interval = dti.interval_code
            WHERE tf.timestamp > NOW() - INTERVAL '$hoursBack hours'
            ON CONFLICT DO NOTHING;
        """)
        
        val countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')",
            Long::class.java
        ) ?: 0L
        
        val recordsLoaded = countAfter - countBefore
        logger.info("Loaded $recordsLoaded new forex records (incremental).")
        return recordsLoaded
    }

    private fun loadCryptoData(): Long {
        logger.info("Loading crypto data into data warehouse...")
        
        val countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')",
            Long::class.java
        ) ?: 0L
        
        // Ensure crypto symbols exist in dimension table
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, currency, is_active)
            SELECT DISTINCT 
                tc.symbol,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO'),
                tc.symbol,
                tc.currency,
                true
            FROM transformed_crypto_data tc
            ON CONFLICT (symbol_code, asset_type_id) DO NOTHING;
        """)
        
        // Load crypto OHLC data into fact table
        // Note: crypto uses daily interval by default
        jdbcTemplate.execute("""
            INSERT INTO fact_ohlc_data (
                symbol_id, asset_type_id, interval_id, timestamp,
                open, high, low, close, volume_from, volume_to, 
                price_change, avg_price, source_table
            )
            SELECT 
                ds.symbol_id,
                ds.asset_type_id,
                (SELECT interval_id FROM dim_time_interval WHERE interval_code = '1day'),
                tc.timestamp,
                tc.open,
                tc.high,
                tc.low,
                tc.close,
                tc.volume_from,
                tc.volume_to,
                tc.price_change,
                tc.avg_price,
                'transformed_crypto_data'
            FROM transformed_crypto_data tc
            JOIN dim_symbol ds ON tc.symbol = ds.symbol_code 
                AND ds.asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')
            ON CONFLICT DO NOTHING;
        """)
        
        val countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')",
            Long::class.java
        ) ?: 0L
        
        val recordsLoaded = countAfter - countBefore
        logger.info("Loaded $recordsLoaded new crypto records into data warehouse (total: $countAfter).")
        return recordsLoaded
    }

    private fun loadCryptoDataIncremental(hoursBack: Int = 2): Long {
        logger.info("Loading incremental crypto data (last $hoursBack hours)...")
        
        val countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')",
            Long::class.java
        ) ?: 0L
        
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, currency, is_active)
            SELECT DISTINCT 
                tc.symbol,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO'),
                tc.symbol,
                tc.currency,
                true
            FROM transformed_crypto_data tc
            WHERE tc.timestamp > NOW() - INTERVAL '$hoursBack hours'
            ON CONFLICT (symbol_code, asset_type_id) DO NOTHING;
        """)
        
        jdbcTemplate.execute("""
            INSERT INTO fact_ohlc_data (
                symbol_id, asset_type_id, interval_id, timestamp,
                open, high, low, close, volume_from, volume_to, 
                price_change, avg_price, source_table
            )
            SELECT 
                ds.symbol_id,
                ds.asset_type_id,
                (SELECT interval_id FROM dim_time_interval WHERE interval_code = '1day'),
                tc.timestamp,
                tc.open,
                tc.high,
                tc.low,
                tc.close,
                tc.volume_from,
                tc.volume_to,
                tc.price_change,
                tc.avg_price,
                'transformed_crypto_data'
            FROM transformed_crypto_data tc
            JOIN dim_symbol ds ON tc.symbol = ds.symbol_code 
                AND ds.asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')
            WHERE tc.timestamp > NOW() - INTERVAL '$hoursBack hours'
            ON CONFLICT DO NOTHING;
        """)
        
        val countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')",
            Long::class.java
        ) ?: 0L
        
        val recordsLoaded = countAfter - countBefore
        logger.info("Loaded $recordsLoaded new crypto records (incremental).")
        return recordsLoaded
    }

    /**
     * Refresh all materialized views (public interface)
     * This is scheduled to run every 30 minutes
     */
    @Scheduled(cron = "0 */30 * * * *") // Every 30 minutes
    fun refreshMaterializedViews(): Map<String, Any> {
        return refreshMaterializedViewsInternal()
    }
    
    /**
     * Internal method to refresh materialized views and return stats
     */
    private fun refreshMaterializedViewsInternal(): Map<String, Any> {
        logger.info("Refreshing materialized views...")
        val startTime = LocalDateTime.now()
        
        val viewStats = mutableMapOf<String, String>()
        val errors = mutableListOf<String>()
        
        val views = listOf(
            "mv_daily_ohlc_summary",
            "mv_hourly_ohlc_summary",
            "mv_asset_type_summary",
            "mv_symbol_analytics"
        )
        
        for (viewName in views) {
            try {
                jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY $viewName;")
                viewStats[viewName] = "refreshed"
                logger.info("Refreshed '$viewName'")
            } catch (e: Exception) {
                val errorMsg = "Failed to refresh $viewName: ${e.message}"
                logger.error(errorMsg, e)
                viewStats[viewName] = "error: ${e.message}"
                errors.add(errorMsg)
            }
        }
        
        val endTime = LocalDateTime.now()
        val duration = java.time.Duration.between(startTime, endTime)
        
        val result = mutableMapOf<String, Any>(
            "viewsProcessed" to views.size,
            "viewsRefreshed" to viewStats.count { it.value == "refreshed" },
            "viewsWithErrors" to errors.size,
            "details" to viewStats,
            "durationSeconds" to duration.seconds
        )
        
        if (errors.isNotEmpty()) {
            result["errors"] = errors
            val errorSummary = "Materialized views refresh completed with ${errors.size} error(s)"
            logger.error(errorSummary)
        } else {
            logger.info("Materialized views refresh completed successfully.")
        }
        
        return result
    }
    
    /**
     * Get the date range of data in the warehouse
     */
    private fun getDataDateRange(): Map<String, String?> {
        return try {
            val result = jdbcTemplate.queryForMap(
                "SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM fact_ohlc_data"
            )
            mapOf(
                "earliest" to (result["earliest"] as? java.sql.Timestamp)?.toLocalDateTime()?.format(DATE_FORMATTER),
                "latest" to (result["latest"] as? java.sql.Timestamp)?.toLocalDateTime()?.format(DATE_FORMATTER)
            )
        } catch (e: Exception) {
            logger.error("Error getting date range: ${e.message}")
            mapOf("earliest" to null, "latest" to null)
        }
    }

    /**
     * Get statistics about the data warehouse
     */
    fun getWarehouseStatistics(): Map<String, Any> {
        val stats = mutableMapOf<String, Any>()
        
        try {
            val totalRecords = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM fact_ohlc_data",
                Long::class.java
            ) ?: 0L
            stats["total_records"] = totalRecords
            
            val symbolCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dim_symbol WHERE is_active = true",
                Int::class.java
            ) ?: 0
            stats["active_symbols"] = symbolCount
            
            val assetTypes = jdbcTemplate.queryForList(
                "SELECT asset_type_code, COUNT(*) as count FROM fact_ohlc_data f JOIN dim_asset_type at ON f.asset_type_id = at.asset_type_id GROUP BY asset_type_code",
                Map::class.java
            )
            stats["records_by_asset_type"] = assetTypes
            
            val dateRange = jdbcTemplate.queryForMap(
                "SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM fact_ohlc_data"
            )
            stats["date_range"] = dateRange
            
        } catch (e: Exception) {
            logger.error("Error getting warehouse statistics: ${e.message}")
        }
        
        return stats
    }
}
