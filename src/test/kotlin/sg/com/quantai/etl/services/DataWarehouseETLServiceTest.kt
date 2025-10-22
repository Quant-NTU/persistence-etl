package sg.com.quantai.etl.services

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.*
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DataWarehouseETLServiceTest {

    @Mock
    private lateinit var jdbcTemplate: JdbcTemplate

    private lateinit var dataWarehouseETLService: DataWarehouseETLService

    @BeforeEach
    fun setup() {
        MockitoAnnotations.openMocks(this)
        dataWarehouseETLService = DataWarehouseETLService(jdbcTemplate)
    }

    @Test
    fun `loadAllDataToWarehouse should load all asset types`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then - Should insert symbols for all asset types
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("INSERT INTO dim_symbol") && contains("'STOCK'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("INSERT INTO dim_symbol") && contains("'FOREX'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("INSERT INTO dim_symbol") && contains("'CRYPTO'")
        })
    }

    @Test
    fun `loadAllDataToWarehouse should load fact data from all sources`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then - Should load data from all transformed tables
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("FROM transformed_stock_data")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("FROM transformed_forex_data")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("FROM transformed_crypto_data")
        })
    }

    @Test
    fun `loadAllDataToWarehouse should refresh materialized views`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then
        verify(jdbcTemplate, times(4)).execute(argThat<String> { 
            contains("REFRESH MATERIALIZED VIEW CONCURRENTLY")
        })
    }

    @Test
    fun `incrementalLoadToWarehouse should only load recent data`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        dataWarehouseETLService.incrementalLoadToWarehouse()

        // Then - Should filter by timestamp
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("WHERE") && contains("timestamp > NOW() - INTERVAL '2 hours'")
        })
    }

    @Test
    fun `incrementalLoadToWarehouse should load all asset types`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        dataWarehouseETLService.incrementalLoadToWarehouse()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("transformed_stock_data") && contains("INTERVAL '2 hours'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("transformed_forex_data") && contains("INTERVAL '2 hours'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("transformed_crypto_data") && contains("INTERVAL '2 hours'")
        })
    }

    @Test
    fun `refreshMaterializedViews should refresh all views`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        dataWarehouseETLService.refreshMaterializedViews()

        // Then
        verify(jdbcTemplate).execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_ohlc_summary;")
        verify(jdbcTemplate).execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_ohlc_summary;")
        verify(jdbcTemplate).execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_asset_type_summary;")
        verify(jdbcTemplate).execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_symbol_analytics;")
    }

    @Test
    fun `refreshMaterializedViews should handle individual view refresh failures`() {
        // Given
        whenever(jdbcTemplate.execute(argThat<String> { this.contains("mv_daily_ohlc_summary") }))
            .thenThrow(RuntimeException("Failed to refresh"))
        whenever(jdbcTemplate.execute(argThat<String> { !this.contains("mv_daily_ohlc_summary") }))
            .then { }

        // When
        dataWarehouseETLService.refreshMaterializedViews()

        // Then - Should still attempt to refresh other views
        verify(jdbcTemplate).execute(argThat<String> { this.contains("mv_hourly_ohlc_summary") })
        verify(jdbcTemplate).execute(argThat<String> { this.contains("mv_asset_type_summary") })
        verify(jdbcTemplate).execute(argThat<String> { this.contains("mv_symbol_analytics") })
    }

    @Test
    fun `getWarehouseStatistics should return statistics map`() {
        // Given
        whenever(jdbcTemplate.queryForObject(
            argThat<String> { contains("SELECT COUNT(*) FROM fact_ohlc_data") },
            eq(Long::class.java)
        )).thenReturn(1000L)

        whenever(jdbcTemplate.queryForObject(
            argThat<String> { contains("SELECT COUNT(*) FROM dim_symbol") },
            eq(Int::class.java)
        )).thenReturn(25)

        whenever(jdbcTemplate.queryForList(
            argThat<String> { contains("records_by_asset_type") || contains("GROUP BY asset_type_code") },
            eq(Map::class.java)
        )).thenReturn(listOf(
            mapOf("asset_type_code" to "STOCK", "count" to 600),
            mapOf("asset_type_code" to "FOREX", "count" to 300),
            mapOf("asset_type_code" to "CRYPTO", "count" to 100)
        ))

        whenever(jdbcTemplate.queryForMap(
            argThat<String> { contains("MIN(timestamp)") && contains("MAX(timestamp)") }
        )).thenReturn(mapOf(
            "earliest" to "2024-01-01",
            "latest" to "2024-12-31"
        ))

        // When
        val stats = dataWarehouseETLService.getWarehouseStatistics()

        // Then
        assertNotNull(stats)
        assertEquals(1000L, stats["total_records"])
        assertEquals(25, stats["active_symbols"])
        assertTrue(stats.containsKey("records_by_asset_type"))
        assertTrue(stats.containsKey("date_range"))
    }

    @Test
    fun `getWarehouseStatistics should handle query failures gracefully`() {
        // Given
        whenever(jdbcTemplate.queryForObject(any<String>(), any<Class<*>>()))
            .thenThrow(RuntimeException("Database error"))

        // When
        val stats = dataWarehouseETLService.getWarehouseStatistics()

        // Then - Should return empty map instead of throwing
        assertNotNull(stats)
    }

    @Test
    fun `stock data load should use ON CONFLICT DO NOTHING`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then - Should handle conflicts gracefully
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("ON CONFLICT") && contains("DO NOTHING")
        })
    }

    @Test
    fun `forex data load should join with dimension tables`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("JOIN dim_symbol") && 
            contains("JOIN dim_time_interval") &&
            contains("transformed_forex_data")
        })
    }

    @Test
    fun `crypto data load should include volume_from and volume_to`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("volume_from") && 
            contains("volume_to") &&
            contains("transformed_crypto_data")
        })
    }

    @Test
    fun `should set source_table in fact data`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }
        whenever(jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)))
            .thenReturn(100)

        // When
        dataWarehouseETLService.loadAllDataToWarehouse()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("'transformed_stock_data'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("'transformed_forex_data'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("'transformed_crypto_data'")
        })
    }
}

