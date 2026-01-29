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
    fun `should fetch and store historical stock data by date successfully`() {
        // Arrange
        `when`(stockService.getSupportedIntervals())
            .thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))

        doNothing().`when`(stockService).fetchAndStoreHistoricalDataByDate(
            symbol = "AAPL",
            interval = "1day",
            startDate = "2025-10-01",
            endDate = "2025-10-31"
        )

        // Act
        val response = controller.fetchAndStoreHistoricalDataByDate(
            symbol = "AAPL",
            startDate = "2025-10-01",
            endDate = "2025-10-31",
            interval = "1day"
        )

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(
            "Historical data for AAPL from 2025-10-01 to 2025-10-31 successfully stored!",
            response.body
        )
    }

    @Test
    fun `fetchAndStoreHistoricalDataByDate returns INTERNAL_SERVER_ERROR on exception`() {
        // Arrange
        `when`(stockService.getSupportedIntervals())
            .thenReturn(listOf("1min", "5min", "15min", "1h", "4h", "1day"))

        `when`(
            stockService.fetchAndStoreHistoricalDataByDate(
                symbol = "AAPL",
                interval = "1day",
                startDate = "2025-10-01",
                endDate = "2025-10-31"
            )
        ).thenThrow(RuntimeException("Mocked exception"))

        // Act
        val response = controller.fetchAndStoreHistoricalDataByDate(
            symbol = "AAPL",
            startDate = "2025-10-01",
            endDate = "2025-10-31",
            interval = "1day"
        )

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assert(response.body!!.contains("Error storing historical data for AAPL"))
    }



}

