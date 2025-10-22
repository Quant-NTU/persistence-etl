package sg.com.quantai.etl.services

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.*
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertNotNull

class DataWarehouseInitializerTest {

    @Mock
    private lateinit var jdbcTemplate: JdbcTemplate

    private lateinit var dataWarehouseInitializer: DataWarehouseInitializer

    @BeforeEach
    fun setup() {
        MockitoAnnotations.openMocks(this)
        dataWarehouseInitializer = DataWarehouseInitializer(jdbcTemplate)
    }

    @Test
    fun `should create dimension tables successfully`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then - Verify dimension tables are created
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS dim_asset_type")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS dim_symbol")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS dim_time_interval")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS dim_exchange")
        })
    }

    @Test
    fun `should insert default asset types`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("INSERT INTO dim_asset_type") && 
            contains("'STOCK'") && 
            contains("'FOREX'") && 
            contains("'CRYPTO'")
        })
    }

    @Test
    fun `should insert default time intervals`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("INSERT INTO dim_time_interval") && 
            contains("'1min'") && 
            contains("'1day'")
        })
    }

    @Test
    fun `should create fact table`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS fact_ohlc_data")
        })
    }

    @Test
    fun `should create hypertable for fact table`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("create_hypertable('fact_ohlc_data'")
        })
    }

    @Test
    fun `should create materialized views`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_ohlc_summary")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_ohlc_summary")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_asset_type_summary")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_symbol_analytics")
        })
    }

    @Test
    fun `should create indexes for performance`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE INDEX IF NOT EXISTS idx_dim_symbol")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE INDEX IF NOT EXISTS idx_fact_ohlc")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE INDEX IF NOT EXISTS idx_mv_")
        })
    }

    @Test
    fun `should create continuous aggregates`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_daily_ohlc")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_hourly_ohlc")
        })
    }

    @Test
    fun `should add refresh policies for continuous aggregates`() {
        // Given
        whenever(jdbcTemplate.execute(any<String>())).then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("add_continuous_aggregate_policy('cagg_daily_ohlc'")
        })
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("add_continuous_aggregate_policy('cagg_hourly_ohlc'")
        })
    }

    @Test
    fun `should handle hypertable creation failure gracefully`() {
        // Given
        whenever(jdbcTemplate.execute(argThat<String> { contains("create_hypertable") }))
            .thenThrow(RuntimeException("TimescaleDB not available"))
        whenever(jdbcTemplate.execute(argThat<String> { !contains("create_hypertable") }))
            .then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then - Should not throw exception, just log warning
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS fact_ohlc_data")
        })
    }

    @Test
    fun `should handle continuous aggregate creation failure gracefully`() {
        // Given
        whenever(jdbcTemplate.execute(argThat<String> { contains("timescaledb.continuous") }))
            .thenThrow(RuntimeException("TimescaleDB extension not enabled"))
        whenever(jdbcTemplate.execute(argThat<String> { !contains("timescaledb.continuous") }))
            .then { }

        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()
        commandLineRunner.run()

        // Then - Should not throw exception, just log warning
        verify(jdbcTemplate, atLeastOnce()).execute(argThat<String> { 
            contains("CREATE TABLE IF NOT EXISTS fact_ohlc_data")
        })
    }

    @Test
    fun `should return non-null CommandLineRunner`() {
        // When
        val commandLineRunner = dataWarehouseInitializer.dataWarehouseRunner()

        // Then
        assertNotNull(commandLineRunner)
    }
}

