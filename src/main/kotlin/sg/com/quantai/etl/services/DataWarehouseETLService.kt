package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class DataWarehouseETLService(
    private val jdbcTemplate: JdbcTemplate
) {

    private val logger: Logger = LoggerFactory.getLogger(DataWarehouseETLService::class.java)

    /**
     * Load all data from source tables into the data warehouse
     * This should be run initially to populate the warehouse
     */
    @Transactional
    fun loadAllDataToWarehouse() {
        logger.info("Starting full data warehouse load...")
        
        loadStockData()
        loadForexData()
        loadCryptoData()
        refreshMaterializedViews()
        
        logger.info("Full data warehouse load completed.")
    }

    /**
     * Incremental load - scheduled to run every hour
     * Loads only recent data (last 2 hours) to keep warehouse up to date
     */
    @Scheduled(cron = "0 0 * * * *") // Every hour at minute 0
    @Transactional
    fun incrementalLoadToWarehouse() {
        logger.info("Starting incremental data warehouse load...")
        
        loadStockDataIncremental()
        loadForexDataIncremental()
        loadCryptoDataIncremental()
        refreshMaterializedViews()
        
        logger.info("Incremental data warehouse load completed.")
    }

    private fun loadStockData() {
        logger.info("Loading stock data into data warehouse...")
        
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
        
        val count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK')",
            Int::class.java
        )
        logger.info("Loaded $count stock records into data warehouse.")
    }

    private fun loadStockDataIncremental() {
        logger.info("Loading incremental stock data...")
        
        // Load only recent data (last 2 hours)
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, is_active)
            SELECT DISTINCT 
                ts.symbol,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'STOCK'),
                ts.symbol,
                true
            FROM transformed_stock_data ts
            WHERE ts.timestamp > NOW() - INTERVAL '2 hours'
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
            WHERE ts.timestamp > NOW() - INTERVAL '2 hours'
            ON CONFLICT DO NOTHING;
        """)
    }

    private fun loadForexData() {
        logger.info("Loading forex data into data warehouse...")
        
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
        
        val count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX')",
            Int::class.java
        )
        logger.info("Loaded $count forex records into data warehouse.")
    }

    private fun loadForexDataIncremental() {
        logger.info("Loading incremental forex data...")
        
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, is_active)
            SELECT DISTINCT 
                tf.currency_pair,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'FOREX'),
                tf.currency_pair,
                true
            FROM transformed_forex_data tf
            WHERE tf.timestamp > NOW() - INTERVAL '2 hours'
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
            WHERE tf.timestamp > NOW() - INTERVAL '2 hours'
            ON CONFLICT DO NOTHING;
        """)
    }

    private fun loadCryptoData() {
        logger.info("Loading crypto data into data warehouse...")
        
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
        
        val count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM fact_ohlc_data WHERE asset_type_id = (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO')",
            Int::class.java
        )
        logger.info("Loaded $count crypto records into data warehouse.")
    }

    private fun loadCryptoDataIncremental() {
        logger.info("Loading incremental crypto data...")
        
        jdbcTemplate.execute("""
            INSERT INTO dim_symbol (symbol_code, asset_type_id, symbol_name, currency, is_active)
            SELECT DISTINCT 
                tc.symbol,
                (SELECT asset_type_id FROM dim_asset_type WHERE asset_type_code = 'CRYPTO'),
                tc.symbol,
                tc.currency,
                true
            FROM transformed_crypto_data tc
            WHERE tc.timestamp > NOW() - INTERVAL '2 hours'
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
            WHERE tc.timestamp > NOW() - INTERVAL '2 hours'
            ON CONFLICT DO NOTHING;
        """)
    }

    /**
     * Refresh all materialized views
     * This is scheduled to run every 30 minutes
     */
    @Scheduled(cron = "0 */30 * * * *") // Every 30 minutes
    fun refreshMaterializedViews() {
        logger.info("Refreshing materialized views...")
        
        try {
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_ohlc_summary;")
            logger.info("Refreshed 'mv_daily_ohlc_summary'")
        } catch (e: Exception) {
            logger.error("Failed to refresh mv_daily_ohlc_summary: ${e.message}")
        }
        
        try {
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_ohlc_summary;")
            logger.info("Refreshed 'mv_hourly_ohlc_summary'")
        } catch (e: Exception) {
            logger.error("Failed to refresh mv_hourly_ohlc_summary: ${e.message}")
        }
        
        try {
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_asset_type_summary;")
            logger.info("Refreshed 'mv_asset_type_summary'")
        } catch (e: Exception) {
            logger.error("Failed to refresh mv_asset_type_summary: ${e.message}")
        }
        
        try {
            jdbcTemplate.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_symbol_analytics;")
            logger.info("Refreshed 'mv_symbol_analytics'")
        } catch (e: Exception) {
            logger.error("Failed to refresh mv_symbol_analytics: ${e.message}")
        }
        
        logger.info("Materialized views refresh completed.")
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
