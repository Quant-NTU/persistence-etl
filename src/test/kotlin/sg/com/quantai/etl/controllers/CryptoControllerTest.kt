package sg.com.quantai.etl.controllers

import com.fasterxml.jackson.databind.JsonNode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import org.springframework.test.context.junit.jupiter.SpringExtension
import sg.com.quantai.etl.services.CryptoService

@ExtendWith(SpringExtension::class)
class CryptoControllerTest {

    @Mock
    private lateinit var cryptoService: CryptoService

    @InjectMocks
    private lateinit var cryptoController: CryptoController

    @Test
    fun `should fetch current price successfully`() {
        val mockResponse = mock(JsonNode::class.java)
        `when`(cryptoService.fetchCryptoPrice("BTC", "USD")).thenReturn(mockResponse)

        val response = cryptoController.getCryptoPrice("BTC", "USD")

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockResponse, response.body)
    }

    @Test
    fun `should handle error while fetching current price`() {
        `when`(cryptoService.fetchCryptoPrice("BTC", "USD")).thenReturn(null)

        val response = cryptoController.getCryptoPrice("BTC", "USD")

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
    }

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
}