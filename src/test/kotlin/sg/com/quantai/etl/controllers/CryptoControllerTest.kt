package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import org.springframework.test.context.junit.jupiter.SpringExtension
import sg.com.quantai.etl.services.CryptoService
import sg.com.quantai.etl.services.CryptoTransformationService

@ExtendWith(SpringExtension::class)
class CryptoControllerTest {

    @Mock
    private lateinit var cryptoService: CryptoService

    @Mock
    private lateinit var transformationService: CryptoTransformationService

    @InjectMocks
    private lateinit var cryptoController: CryptoController

    @Test
    fun `should fetch and store historical data successfully`() {
        doNothing().`when`(cryptoService).fetchAndStoreHistoricalData("BTC", "USD", 10)

        val response = cryptoController.fetchAndStoreHistoricalData("BTC", "USD", 10)

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for BTC-USD successfully stored!", response.body)
    }

    @Test
    fun `should handle error while storing historical data`() {
        doThrow(RuntimeException("Mocked exception"))
            .`when`(cryptoService).fetchAndStoreHistoricalData("BTC", "USD", 10)

        val response = cryptoController.fetchAndStoreHistoricalData("BTC", "USD", 10)

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
    }

    @Test
    fun `should trigger data transformation`() {
        doNothing().`when`(transformationService).transformData()

        val response = cryptoController.triggerTransformation()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Transformation triggered!", response.body)
    }

    @Test
    fun `should fetch top 10 crypto symbols`() {
        val mockSymbols = listOf("BTC", "ETH", "BNB")
        `when`(cryptoService.getTopCryptoSymbols()).thenReturn(mockSymbols)

        val response = cryptoController.getTopCryptoSymbols()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockSymbols, response.body)
    }

    @Test
    fun `should fetch and store historical data for top symbols`() {
        doNothing().`when`(cryptoService).storeHistoricalDataForTopSymbols()

        val response = cryptoController.storeHistoricalDataForTopSymbols()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for top symbols stored successfully!", response.body)
    }

    @Test
    fun `should handle error while storing historical data for top symbols`() {
        doThrow(RuntimeException("Mocked exception"))
            .`when`(cryptoService).storeHistoricalDataForTopSymbols()

        val response = cryptoController.storeHistoricalDataForTopSymbols()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
    }

    @Test
    fun `should fetch and store historical data by date successfully`() {
        doNothing().`when`(cryptoService).fetchAndStoreHistoricalDataByDate(
            "BTC", "USD", "2025-10-01", "2025-10-31"
        )

        val response = cryptoController.fetchAndStoreHistoricalDataByDate(
            "BTC", "USD", "2025-10-01", "2025-10-31"
        )

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(
            "Historical data for BTC-USD from 2025-10-01 to 2025-10-31 successfully stored!",
            response.body
        )
    }

    @Test
    fun `should handle error while storing historical data by date`() {
        doThrow(RuntimeException("Mocked exception"))
            .`when`(cryptoService).fetchAndStoreHistoricalDataByDate(
                "BTC", "USD", "2025-10-01", "2025-10-31"
            )

        val response = cryptoController.fetchAndStoreHistoricalDataByDate(
            "BTC", "USD", "2025-10-01", "2025-10-31"
        )

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assert(response.body!!.contains("Error storing historical data: Mocked exception"))
    }

    @Test
    fun `should return close price successfully`() {
        // Arrange
        `when`(
            cryptoService.getClosePriceByDate("BTC", "USD", "2025-12-03")
        ).thenReturn(50000.0)

        // Act
        val response = cryptoController.getPriceByDate(
            symbol = "BTC",
            date = "2025-12-03",
            currency = "USD"
        )

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)

        val body = response.body as Map<*, *>
        assertEquals("BTC", body["symbol"])
        assertEquals("USD", body["currency"])
        assertEquals("2025-12-03", body["date"])
        assertEquals(50000.0, body["close"])
    }

    @Test
    fun `should return 404 when close price not found`() {
        // Arrange
        `when`(
            cryptoService.getClosePriceByDate("BTC", "USD", "2025-12-03")
        ).thenReturn(null)

        // Act
        val response = cryptoController.getPriceByDate(
            symbol = "BTC",
            date = "2025-12-03",
            currency = "USD"
        )

        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.statusCode)
        assertEquals("No price found", response.body)
    }

    @Test
    fun `getPriceHistory returns OK with price data on success`() {
        // Arrange
        val mockPriceData = listOf(
            mapOf("date" to "2025-01-15", "open" to 42000.0, "high" to 43000.0, "low" to 41000.0, "close" to 42500.0, "volumeFrom" to 1000.0, "volumeTo" to 42500000.0),
            mapOf("date" to "2025-01-14", "open" to 41000.0, "high" to 42500.0, "low" to 40500.0, "close" to 42000.0, "volumeFrom" to 900.0, "volumeTo" to 37800000.0)
        )
        `when`(cryptoService.fetchPriceHistory("BTC", "USD", 30)).thenReturn(mockPriceData)

        // Act
        val response = cryptoController.getPriceHistory("BTC", "USD", 30)

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("success", body["status"])
        assertEquals("BTC", body["symbol"])
        assertEquals("USD", body["currency"])
        assertEquals(30, body["days"])
        assertEquals(2, body["count"])
        assertEquals(mockPriceData, body["data"])
    }

    @Test
    fun `getPriceHistory returns INTERNAL_SERVER_ERROR on exception`() {
        // Arrange
        `when`(cryptoService.fetchPriceHistory("BTC", "USD", 30))
            .thenThrow(RuntimeException("API error"))

        // Act
        val response = cryptoController.getPriceHistory("BTC", "USD", 30)

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("error", body["status"])
        assert((body["message"] as String).contains("API error"))
    }

    @Test
    fun `getPriceHistory returns empty data when no price data available`() {
        // Arrange
        `when`(cryptoService.fetchPriceHistory("BTC", "USD", 30)).thenReturn(emptyList())

        // Act
        val response = cryptoController.getPriceHistory("BTC", "USD", 30)

        // Assert
        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as Map<*, *>
        assertEquals("success", body["status"])
        assertEquals(0, body["count"])
    }

}