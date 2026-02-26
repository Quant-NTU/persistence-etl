package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.services.ForexService
import sg.com.quantai.etl.services.ForexTransformationService
import org.junit.jupiter.api.Assertions.assertEquals

class ForexControllerTest {

    private val forexService: ForexService = mock(ForexService::class.java)
    private val forexTransformationService: ForexTransformationService = mock(ForexTransformationService::class.java)
    private val controller = ForexController(forexService, forexTransformationService)

    @Test
    fun `getTopForexPairs returns OK with pairs`() {
        val mockPairs = listOf("EUR/USD", "GBP/USD", "USD/JPY")
        `when`(forexService.getTopForexPairs()).thenReturn(mockPairs)

        val response = controller.getTopForexPairs()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockPairs, response.body)
    }

    @Test
    fun `fetchAndStoreHistoricalData returns OK on success`() {
        `when`(forexService.getSupportedIntervals()).thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))
        doNothing().`when`(forexService).fetchAndStoreHistoricalData("EUR/USD", 30, "1day")

        val response = controller.fetchAndStoreHistoricalData("EUR/USD", 30, "1day")

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for EUR/USD (interval: 1day) fetched and stored successfully", response.body)
        verify(forexService, times(1)).fetchAndStoreHistoricalData("EUR/USD", 30, "1day")
    }

    @Test
    fun `fetchAndStoreHistoricalData returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(forexService.getSupportedIntervals()).thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))
        `when`(forexService.fetchAndStoreHistoricalData("EUR/USD", 30, "1day"))
            .thenThrow(RuntimeException("API error"))

        val response = controller.fetchAndStoreHistoricalData("EUR/USD", 30, "1day")

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error fetching data for EUR/USD: API error", response.body)
    }

    @Test
    fun `fetchAndStoreHistoricalDataForTopPairs returns OK on success`() {
        doNothing().`when`(forexService).storeHistoricalDataForTopPairs()

        val response = controller.fetchAndStoreHistoricalDataForTopPairs()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for top forex pairs fetched and stored successfully", response.body)
        verify(forexService, times(1)).storeHistoricalDataForTopPairs()
    }

    @Test
    fun `fetchAndStoreHistoricalDataForTopPairs returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(forexService.storeHistoricalDataForTopPairs())
            .thenThrow(RuntimeException("Network error"))

        val response = controller.fetchAndStoreHistoricalDataForTopPairs()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error fetching top forex data: Network error", response.body)
    }

    @Test
    fun `triggerDataTransformation returns OK on success`() {
        doNothing().`when`(forexTransformationService).transformData()

        val response = controller.triggerDataTransformation()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Forex data transformation completed successfully", response.body)
        verify(forexTransformationService, times(1)).transformData()
    }

    @Test
    fun `triggerDataTransformation returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(forexTransformationService.transformData())
            .thenThrow(RuntimeException("Database error"))

        val response = controller.triggerDataTransformation()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error during forex data transformation: Database error", response.body)
    }

    @Test
    fun `getPriceHistory returns OK with price data on success`() {
        // Arrange
        val mockPriceData = listOf(
            mapOf("date" to "2025-01-15", "open" to 1.08, "high" to 1.09, "low" to 1.07, "close" to 1.085),
            mapOf("date" to "2025-01-14", "open" to 1.07, "high" to 1.08, "low" to 1.06, "close" to 1.08)
        )
        `when`(forexService.fetchPriceHistory("EUR/USD", 30)).thenReturn(mockPriceData)

        // Act
        val response = controller.getPriceHistory("EUR/USD", 30)

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("success", body["status"])
        assertEquals("EUR/USD", body["currencyPair"])
        assertEquals(30, body["days"])
        assertEquals(2, body["count"])
        assertEquals(mockPriceData, body["data"])
    }

    @Test
    fun `getPriceHistory returns INTERNAL_SERVER_ERROR on exception`() {
        // Arrange
        `when`(forexService.fetchPriceHistory("EUR/USD", 30))
            .thenThrow(RuntimeException("API error"))

        // Act
        val response = controller.getPriceHistory("EUR/USD", 30)

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("error", body["status"])
        assert((body["message"] as String).contains("API error"))
    }

    @Test
    fun `getPriceHistory returns empty data when no price data available`() {
        // Arrange
        `when`(forexService.fetchPriceHistory("EUR/USD", 30)).thenReturn(emptyList())

        // Act
        val response = controller.getPriceHistory("EUR/USD", 30)

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("success", body["status"])
        assertEquals(0, body["count"])
    }
}

