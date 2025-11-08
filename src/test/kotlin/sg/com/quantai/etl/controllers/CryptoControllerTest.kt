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

}