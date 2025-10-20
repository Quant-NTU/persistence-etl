package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.services.DataWarehouseETLService
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class DataWarehouseControllerTest {

    @Mock
    private lateinit var dataWarehouseETLService: DataWarehouseETLService

    private lateinit var dataWarehouseController: DataWarehouseController

    @BeforeEach
    fun setup() {
        MockitoAnnotations.openMocks(this)
        dataWarehouseController = DataWarehouseController(dataWarehouseETLService)
    }

    @Test
    fun `loadFullWarehouse should return success when load completes`() {
        // Given
        doNothing().whenever(dataWarehouseETLService).loadAllDataToWarehouse()

        // When
        val response = dataWarehouseController.loadFullWarehouse()

        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertNotNull(response.body)
        assertEquals("success", response.body!!["status"])
        assertEquals("Full data warehouse load completed successfully", response.body!!["message"])
        verify(dataWarehouseETLService).loadAllDataToWarehouse()
    }

    @Test
    fun `loadFullWarehouse should return error when load fails`() {
        // Given
        whenever(dataWarehouseETLService.loadAllDataToWarehouse())
            .thenThrow(RuntimeException("Database connection failed"))

        // When
        val response = dataWarehouseController.loadFullWarehouse()

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertNotNull(response.body)
        assertEquals("error", response.body!!["status"])
        assertTrue(response.body!!["message"].toString().contains("Failed to load data warehouse"))
    }

    @Test
    fun `loadIncrementalWarehouse should return success when load completes`() {
        // Given
        doNothing().whenever(dataWarehouseETLService).incrementalLoadToWarehouse()

        // When
        val response = dataWarehouseController.loadIncrementalWarehouse()

        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertNotNull(response.body)
        assertEquals("success", response.body!!["status"])
        assertEquals("Incremental data warehouse load completed successfully", response.body!!["message"])
        verify(dataWarehouseETLService).incrementalLoadToWarehouse()
    }

    @Test
    fun `loadIncrementalWarehouse should return error when load fails`() {
        // Given
        whenever(dataWarehouseETLService.incrementalLoadToWarehouse())
            .thenThrow(RuntimeException("Query timeout"))

        // When
        val response = dataWarehouseController.loadIncrementalWarehouse()

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertNotNull(response.body)
        assertEquals("error", response.body!!["status"])
        assertTrue(response.body!!["message"].toString().contains("Failed to load incremental data"))
    }

    @Test
    fun `refreshMaterializedViews should return success when refresh completes`() {
        // Given
        doNothing().whenever(dataWarehouseETLService).refreshMaterializedViews()

        // When
        val response = dataWarehouseController.refreshMaterializedViews()

        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertNotNull(response.body)
        assertEquals("success", response.body!!["status"])
        assertEquals("Materialized views refreshed successfully", response.body!!["message"])
        verify(dataWarehouseETLService).refreshMaterializedViews()
    }

    @Test
    fun `refreshMaterializedViews should return error when refresh fails`() {
        // Given
        whenever(dataWarehouseETLService.refreshMaterializedViews())
            .thenThrow(RuntimeException("Concurrent refresh already in progress"))

        // When
        val response = dataWarehouseController.refreshMaterializedViews()

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertNotNull(response.body)
        assertEquals("error", response.body!!["status"])
        assertTrue(response.body!!["message"].toString().contains("Failed to refresh materialized views"))
    }

    @Test
    fun `getWarehouseStatistics should return statistics when available`() {
        // Given
        val mockStats = mapOf(
            "total_records" to 10000L,
            "active_symbols" to 50,
            "records_by_asset_type" to listOf(
                mapOf("asset_type_code" to "STOCK", "count" to 6000),
                mapOf("asset_type_code" to "FOREX", "count" to 3000),
                mapOf("asset_type_code" to "CRYPTO", "count" to 1000)
            ),
            "date_range" to mapOf(
                "earliest" to "2024-01-01",
                "latest" to "2024-12-31"
            )
        )
        whenever(dataWarehouseETLService.getWarehouseStatistics()).thenReturn(mockStats)

        // When
        val response = dataWarehouseController.getWarehouseStatistics()

        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertNotNull(response.body)
        assertEquals("success", response.body!!["status"])
        
        @Suppress("UNCHECKED_CAST")
        val data = response.body!!["data"] as Map<String, Any>
        assertEquals(10000L, data["total_records"])
        assertEquals(50, data["active_symbols"])
        assertTrue(data.containsKey("records_by_asset_type"))
        assertTrue(data.containsKey("date_range"))
        
        verify(dataWarehouseETLService).getWarehouseStatistics()
    }

    @Test
    fun `getWarehouseStatistics should return error when fetch fails`() {
        // Given
        whenever(dataWarehouseETLService.getWarehouseStatistics())
            .thenThrow(RuntimeException("Database unavailable"))

        // When
        val response = dataWarehouseController.getWarehouseStatistics()

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertNotNull(response.body)
        assertEquals("error", response.body!!["status"])
        assertTrue(response.body!!["message"].toString().contains("Failed to fetch statistics"))
    }

    @Test
    fun `getWarehouseStatistics should return empty statistics gracefully`() {
        // Given
        whenever(dataWarehouseETLService.getWarehouseStatistics()).thenReturn(emptyMap())

        // When
        val response = dataWarehouseController.getWarehouseStatistics()

        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertNotNull(response.body)
        assertEquals("success", response.body!!["status"])
        
        @Suppress("UNCHECKED_CAST")
        val data = response.body!!["data"] as Map<String, Any>
        assertTrue(data.isEmpty())
    }

    @Test
    fun `all endpoints should handle null pointer exceptions`() {
        // Given
        whenever(dataWarehouseETLService.loadAllDataToWarehouse())
            .thenThrow(NullPointerException("Unexpected null value"))
        whenever(dataWarehouseETLService.incrementalLoadToWarehouse())
            .thenThrow(NullPointerException("Unexpected null value"))
        whenever(dataWarehouseETLService.refreshMaterializedViews())
            .thenThrow(NullPointerException("Unexpected null value"))
        whenever(dataWarehouseETLService.getWarehouseStatistics())
            .thenThrow(NullPointerException("Unexpected null value"))

        // When
        val fullLoadResponse = dataWarehouseController.loadFullWarehouse()
        val incrementalResponse = dataWarehouseController.loadIncrementalWarehouse()
        val refreshResponse = dataWarehouseController.refreshMaterializedViews()
        val statsResponse = dataWarehouseController.getWarehouseStatistics()

        // Then - All should return error responses, not throw exceptions
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, fullLoadResponse.statusCode)
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, incrementalResponse.statusCode)
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, refreshResponse.statusCode)
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, statsResponse.statusCode)
    }

    @Test
    fun `response format should be consistent across endpoints`() {
        // Given
        doNothing().whenever(dataWarehouseETLService).loadAllDataToWarehouse()
        doNothing().whenever(dataWarehouseETLService).incrementalLoadToWarehouse()
        doNothing().whenever(dataWarehouseETLService).refreshMaterializedViews()
        whenever(dataWarehouseETLService.getWarehouseStatistics()).thenReturn(emptyMap())

        // When
        val responses = listOf(
            dataWarehouseController.loadFullWarehouse(),
            dataWarehouseController.loadIncrementalWarehouse(),
            dataWarehouseController.refreshMaterializedViews(),
            dataWarehouseController.getWarehouseStatistics()
        )

        // Then - All responses should have "status" field
        responses.forEach { response ->
            assertNotNull(response.body)
            assertTrue(response.body!!.containsKey("status"))
            assertEquals("success", response.body!!["status"])
        }
    }
}

