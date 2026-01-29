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
    fun `should fetch and store historical forex data by date successfully`() {
        // Arrange
        `when`(forexService.getSupportedIntervals())
            .thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))

        doNothing().`when`(forexService).fetchAndStoreHistoricalDataByDate(
            currencyPair = "EUR/USD",
            interval = "1day",
            startDate = "2025-10-01",
            endDate = "2025-10-31"
        )

        // Act
        val response = controller.fetchAndStoreHistoricalDataByDate(
            currencyPair = "EUR/USD",
            startDate = "2025-10-01",
            endDate = "2025-10-31",
            interval = "1day"
        )

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(
            "Historical data for EUR/USD (interval=1day) from 2025-10-01 to 2025-10-31 successfully stored!",
            response.body
        )
    }

    @Test
    fun `fetchAndStoreHistoricalDataByDate returns INTERNAL_SERVER_ERROR on exception`() {
        // Arrange
        `when`(forexService.getSupportedIntervals())
            .thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))

        `when`(
            forexService.fetchAndStoreHistoricalDataByDate(
                currencyPair = "EUR/USD",
                interval = "1day",
                startDate = "2025-10-01",
                endDate = "2025-10-31"
            )
        ).thenThrow(RuntimeException("Mocked exception"))

        // Act
        val response = controller.fetchAndStoreHistoricalDataByDate(
            currencyPair = "EUR/USD",
            startDate = "2025-10-01",
            endDate = "2025-10-31",
            interval = "1day"
        )

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assert(response.body!!.contains("Error storing historical data for EUR/USD: Mocked exception"))
    }


}

