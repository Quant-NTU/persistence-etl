package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriBuilder
import reactor.core.publisher.Mono
import java.net.URI
import java.util.function.Function

class TwelveDataApiClientTest {

    private lateinit var twelveDataApiClient: TwelveDataApiClient
    private lateinit var webClientBuilder: WebClient.Builder
    private lateinit var webClient: WebClient
    private val baseUrl = "https://api.twelvedata.com"
    private val apiKey = "test-api-key"
    private val objectMapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        webClientBuilder = Mockito.mock(WebClient.Builder::class.java)
        webClient = Mockito.mock(WebClient::class.java)

        Mockito.`when`(webClientBuilder.baseUrl(baseUrl)).thenReturn(webClientBuilder)
        Mockito.`when`(webClientBuilder.build()).thenReturn(webClient)

        twelveDataApiClient = TwelveDataApiClient(webClientBuilder, baseUrl, apiKey, objectMapper)
    }

    @Test
    fun `fetchTimeSeries should return JsonNode on success`() {
        // Arrange
        val mockResponse = """{"values": [{"datetime": "2024-01-15", "open": 150.0, "high": 155.0, "low": 148.0, "close": 152.0, "volume": 1000000}]}"""
        setupMockWebClient(mockResponse)

        // Act
        val result = twelveDataApiClient.fetchTimeSeries("AAPL", "1day", 30)

        // Assert
        assertNotNull(result)
        assertTrue(result!!.has("values"))
        assertEquals(1, result["values"].size())
    }

    @Test
    fun `fetchTimeSeries should return null on exception`() {
        // Arrange
        val requestHeadersUriSpec = Mockito.mock(WebClient.RequestHeadersUriSpec::class.java)
        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(
            requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())
        ).thenThrow(RuntimeException("Network error"))

        // Act
        val result = twelveDataApiClient.fetchTimeSeries("AAPL", "1day", 30)

        // Assert
        assertNull(result)
    }

    @Test
    fun `fetchTimeSeriesByDateRange should return JsonNode on success`() {
        // Arrange
        val mockResponse = """{"values": [{"datetime": "2024-01-15", "open": 150.0, "high": 155.0, "low": 148.0, "close": 152.0}]}"""
        setupMockWebClient(mockResponse)

        // Act
        val result = twelveDataApiClient.fetchTimeSeriesByDateRange("AAPL", "1day", "2024-01-01", "2024-01-31")

        // Assert
        assertNotNull(result)
        assertTrue(result!!.has("values"))
    }

    @Test
    fun `parsePriceHistory should parse OHLC data correctly`() {
        // Arrange
        val jsonResponse = objectMapper.readTree("""{"values": [{"datetime": "2024-01-15", "open": 150.0, "high": 155.0, "low": 148.0, "close": 152.0}]}""")

        // Act
        val result = twelveDataApiClient.parsePriceHistory(jsonResponse, includeVolume = false)

        // Assert
        assertEquals(1, result.size)
        assertEquals("2024-01-15", result[0]["date"])
        assertEquals(150.0, result[0]["open"])
        assertEquals(155.0, result[0]["high"])
        assertEquals(148.0, result[0]["low"])
        assertEquals(152.0, result[0]["close"])
        assertFalse(result[0].containsKey("volume"))
    }

    @Test
    fun `parsePriceHistory should include volume when requested`() {
        // Arrange
        val jsonResponse = objectMapper.readTree("""{"values": [{"datetime": "2024-01-15", "open": 150.0, "high": 155.0, "low": 148.0, "close": 152.0, "volume": 1000000}]}""")

        // Act
        val result = twelveDataApiClient.parsePriceHistory(jsonResponse, includeVolume = true)

        // Assert
        assertEquals(1, result.size)
        assertTrue(result[0].containsKey("volume"))
        assertEquals(1000000L, result[0]["volume"])
    }

    @Test
    fun `parsePriceHistory should return empty list for null response`() {
        // Act
        val result = twelveDataApiClient.parsePriceHistory(null, includeVolume = false)

        // Assert
        assertTrue(result.isEmpty())
    }

    @Test
    fun `parsePriceHistory should return empty list for response without values`() {
        // Arrange
        val jsonResponse = objectMapper.readTree("""{"status": "ok"}""")

        // Act
        val result = twelveDataApiClient.parsePriceHistory(jsonResponse, includeVolume = false)

        // Assert
        assertTrue(result.isEmpty())
    }

    @Test
    fun `parsePriceHistory should handle multiple data points`() {
        // Arrange
        val jsonResponse = objectMapper.readTree("""{"values": [
            {"datetime": "2024-01-15", "open": 150.0, "high": 155.0, "low": 148.0, "close": 152.0},
            {"datetime": "2024-01-14", "open": 148.0, "high": 151.0, "low": 147.0, "close": 150.0}
        ]}""")

        // Act
        val result = twelveDataApiClient.parsePriceHistory(jsonResponse, includeVolume = false)

        // Assert
        assertEquals(2, result.size)
        assertEquals("2024-01-15", result[0]["date"])
        assertEquals("2024-01-14", result[1]["date"])
    }

    private fun setupMockWebClient(responseBody: String) {
        val requestHeadersUriSpec = Mockito.mock(WebClient.RequestHeadersUriSpec::class.java)
        val requestHeadersSpec = Mockito.mock(WebClient.RequestHeadersSpec::class.java)
        val responseSpec = Mockito.mock(WebClient.ResponseSpec::class.java)

        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(
            requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())
        ).thenReturn(requestHeadersSpec)
        Mockito.`when`(requestHeadersSpec.retrieve()).thenReturn(responseSpec)
        Mockito.`when`(responseSpec.bodyToMono(String::class.java)).thenReturn(
            Mono.just(responseBody)
        )
    }
}
