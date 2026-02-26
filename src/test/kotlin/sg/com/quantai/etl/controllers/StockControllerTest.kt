package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.services.StockService
import sg.com.quantai.etl.services.StockTransformationService
import org.junit.jupiter.api.Assertions.assertEquals

class StockControllerTest {

    private val stockService: StockService = mock(StockService::class.java)
    private val stockTransformationService: StockTransformationService = mock(StockTransformationService::class.java)
    private val controller = StockController(stockService, stockTransformationService)

    @Test
    fun `getTopStockSymbols returns OK with symbols`() {
        val mockSymbols = listOf("AAPL", "MSFT", "GOOGL")
        `when`(stockService.getTopStockSymbols()).thenReturn(mockSymbols)

        val response = controller.getTopStockSymbols()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockSymbols, response.body)
    }

    @Test
    fun `fetchAndStoreHistoricalData returns OK on success`() {
        `when`(stockService.getSupportedIntervals()).thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))
        doNothing().`when`(stockService).fetchAndStoreHistoricalData("AAPL", 30, "1day")

        val response = controller.fetchAndStoreHistoricalData("AAPL", 30, "1day")

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for AAPL (interval: 1day) fetched and stored successfully", response.body)
        verify(stockService, times(1)).fetchAndStoreHistoricalData("AAPL", 30, "1day")
    }

    @Test
    fun `fetchAndStoreHistoricalData returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(stockService.getSupportedIntervals()).thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))
        `when`(stockService.fetchAndStoreHistoricalData("AAPL", 30, "1day"))
            .thenThrow(RuntimeException("API error"))

        val response = controller.fetchAndStoreHistoricalData("AAPL", 30, "1day")

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error fetching data for AAPL: API error", response.body)
    }

    @Test
    fun `fetchAndStoreHistoricalDataForTopSymbols returns OK on success`() {
        doNothing().`when`(stockService).storeHistoricalDataForTopSymbols()

        val response = controller.fetchAndStoreHistoricalDataForTopSymbols()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for top stock symbols fetched and stored successfully", response.body)
        verify(stockService, times(1)).storeHistoricalDataForTopSymbols()
    }

    @Test
    fun `fetchAndStoreHistoricalDataForTopSymbols returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(stockService.storeHistoricalDataForTopSymbols())
            .thenThrow(RuntimeException("Network error"))

        val response = controller.fetchAndStoreHistoricalDataForTopSymbols()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error fetching top stock data: Network error", response.body)
    }

    @Test
    fun `triggerDataTransformation returns OK on success`() {
        doNothing().`when`(stockTransformationService).transformData()

        val response = controller.triggerDataTransformation()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Stock data transformation completed successfully", response.body)
        verify(stockTransformationService, times(1)).transformData()
    }

    @Test
    fun `triggerDataTransformation returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(stockTransformationService.transformData())
            .thenThrow(RuntimeException("Database error"))

        val response = controller.triggerDataTransformation()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error during stock data transformation: Database error", response.body)
    }

    @Test
    fun `should fetch and store stock historical data by date successfully`() {
        // Arrange
        `when`(stockService.getSupportedIntervals()).thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))
        doNothing().`when`(stockService).fetchAndStoreHistoricalDataByDate(
            symbol = "AAPL",
            interval = "1day",
            startDate = "2025-10-01",
            endDate = "2025-10-31"
        )

        // Act
        val response = controller.fetchAndStoreHistoricalDataByDate(
            "AAPL", "2025-10-01", "2025-10-31", "1day"
        )

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(
            "Historical data for AAPL (interval=1day) from 2025-10-01 to 2025-10-31 successfully stored!",
            response.body
        )
    }

    @Test
    fun `should handle error while storing stock historical data by date`() {
        // Arrange
        `when`(stockService.getSupportedIntervals()).thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))
        doThrow(RuntimeException("Mocked exception"))
            .`when`(stockService).fetchAndStoreHistoricalDataByDate(
                symbol = "AAPL",
                interval = "1day",
                startDate = "2025-10-01",
                endDate = "2025-10-31"
            )

        // Act
        val response = controller.fetchAndStoreHistoricalDataByDate(
            "AAPL", "2025-10-01", "2025-10-31", "1day"
        )

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assert(response.body!!.contains("Error storing historical data"))
    }

    @Test
    fun `getPriceHistory returns OK with price data on success`() {
        // Arrange
        val mockPriceData = listOf(
            mapOf("date" to "2025-01-15", "open" to 150.0, "high" to 155.0, "low" to 148.0, "close" to 152.0, "volume" to 1000000L),
            mapOf("date" to "2025-01-14", "open" to 148.0, "high" to 151.0, "low" to 147.0, "close" to 150.0, "volume" to 900000L)
        )
        `when`(stockService.fetchPriceHistory("AAPL", 30)).thenReturn(mockPriceData)

        // Act
        val response = controller.getPriceHistory("AAPL", 30)

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("success", body["status"])
        assertEquals("AAPL", body["symbol"])
        assertEquals(30, body["days"])
        assertEquals(2, body["count"])
        assertEquals(mockPriceData, body["data"])
    }

    @Test
    fun `getPriceHistory returns INTERNAL_SERVER_ERROR on exception`() {
        // Arrange
        `when`(stockService.fetchPriceHistory("AAPL", 30))
            .thenThrow(RuntimeException("API error"))

        // Act
        val response = controller.getPriceHistory("AAPL", 30)

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("error", body["status"])
        assert((body["message"] as String).contains("API error"))
    }

    @Test
    fun `getSP500DailyPrices returns OK with price data on success`() {
        // Arrange
        val mockPriceData = listOf(
            mapOf("date" to "2025-01-15", "open" to 500.0, "high" to 505.0, "low" to 498.0, "close" to 502.0, "volume" to 5000000L)
        )
        `when`(stockService.fetchSP500DailyPrices(30)).thenReturn(mockPriceData)

        // Act
        val response = controller.getSP500DailyPrices(30)

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("success", body["status"])
        assertEquals("SPY", body["symbol"])
        assertEquals(1, body["count"])
    }

    @Test
    fun `getSP500DailyPrices returns INTERNAL_SERVER_ERROR on exception`() {
        // Arrange
        `when`(stockService.fetchSP500DailyPrices(30))
            .thenThrow(RuntimeException("Network error"))

        // Act
        val response = controller.getSP500DailyPrices(30)

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("error", body["status"])
    }

}

