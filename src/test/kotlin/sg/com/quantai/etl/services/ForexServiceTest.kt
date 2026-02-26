package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriBuilder
import reactor.core.publisher.Mono
import java.net.URI
import java.util.function.Function
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows

class ForexServiceTest {

    private lateinit var forexService: ForexService
    private lateinit var webClientBuilder: WebClient.Builder
    private lateinit var webClient: WebClient
    private lateinit var jdbcTemplate: JdbcTemplate
    private lateinit var twelveDataApiClient: TwelveDataApiClient
    private val twelveDataBaseUrl = "https://api.twelvedata.com"
    private val apiKey = "test-api-key"
    private val objectMapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        webClientBuilder = Mockito.mock(WebClient.Builder::class.java)
        webClient = Mockito.mock(WebClient::class.java)
        jdbcTemplate = Mockito.mock(JdbcTemplate::class.java)
        twelveDataApiClient = Mockito.mock(TwelveDataApiClient::class.java)

        val requestHeadersUriSpec = Mockito.mock(WebClient.RequestHeadersUriSpec::class.java)
        val requestHeadersSpec = Mockito.mock(WebClient.RequestHeadersSpec::class.java)
        val responseSpec = Mockito.mock(WebClient.ResponseSpec::class.java)

        // WebClient mock setup
        Mockito.`when`(webClientBuilder.baseUrl(twelveDataBaseUrl)).thenReturn(webClientBuilder)
        Mockito.`when`(webClientBuilder.build()).thenReturn(webClient)
        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(
            requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())
        ).thenReturn(requestHeadersSpec)
        Mockito.`when`(requestHeadersSpec.retrieve()).thenReturn(responseSpec)

        // Mock API response
        Mockito.`when`(responseSpec.bodyToMono(String::class.java)).thenReturn(
            Mono.just("""{"values": [{"datetime": "2024-01-15", "open": "1.0850", "high": "1.0890", "low": "1.0820", "close": "1.0875"}]}""")
        )

        // Initialize the service
        forexService = ForexService(webClientBuilder, twelveDataBaseUrl, apiKey, objectMapper, jdbcTemplate, twelveDataApiClient)
    }

    @Test
    fun `should return top forex pairs`() {
        val pairs = forexService.getTopForexPairs()
        
        assertEquals(10, pairs.size)
        assertEquals("EUR/USD", pairs[0])
        assertEquals("GBP/USD", pairs[1])
        assertEquals("USD/JPY", pairs[2])
    }

    @Test
    fun `should fetch and store historical data successfully`() {
        // Mock batch update for the new implementation
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenReturn(intArrayOf(1)) // Successful batch insertion

        // Call the method
        forexService.fetchAndStoreHistoricalData("EUR/USD", 5, "1day")

        // Verify batch update was called
        Mockito.verify(jdbcTemplate, Mockito.times(1)).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `should store historical data for top pairs`() {
        // Mock batch update for the new implementation
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenReturn(intArrayOf(1)) // Successful batch insertion

        // Call the method
        forexService.storeHistoricalDataForTopPairs()

        // Verify that batch update was called for multiple pairs (10 top pairs)
        Mockito.verify(jdbcTemplate, Mockito.atLeast(1)).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `fetchPriceHistory should return price data on success`() {
        // Arrange
        val mockJsonResponse = objectMapper.readTree("""{"values": [{"datetime": "2024-01-15", "open": 1.085, "high": 1.089, "low": 1.082, "close": 1.0875}]}""")
        val mockPriceData = listOf(
            mapOf("date" to "2024-01-15", "open" to 1.085, "high" to 1.089, "low" to 1.082, "close" to 1.0875)
        )
        
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("EUR/USD", "1day", 30)).thenReturn(mockJsonResponse)
        Mockito.`when`(twelveDataApiClient.parsePriceHistory(mockJsonResponse, false)).thenReturn(mockPriceData)

        // Act
        val result = forexService.fetchPriceHistory("EUR/USD", 30)

        // Assert
        assertEquals(1, result.size)
        assertEquals("2024-01-15", result[0]["date"])
        assertEquals(1.0875, result[0]["close"])
        Mockito.verify(twelveDataApiClient).fetchTimeSeries("EUR/USD", "1day", 30)
        Mockito.verify(twelveDataApiClient).parsePriceHistory(mockJsonResponse, false)
    }

    @Test
    fun `fetchPriceHistory should throw exception when API returns null`() {
        // Arrange
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("EUR/USD", "1day", 30)).thenReturn(null)

        // Act & Assert
        val exception = assertThrows(RuntimeException::class.java) {
            forexService.fetchPriceHistory("EUR/USD", 30)
        }
        assertEquals("Failed to fetch price history for EUR/USD", exception.message)
    }

    @Test
    fun `fetchPriceHistory should return empty list when no data available`() {
        // Arrange
        val mockJsonResponse = objectMapper.readTree("""{"values": []}""")
        
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("EUR/USD", "1day", 30)).thenReturn(mockJsonResponse)
        Mockito.`when`(twelveDataApiClient.parsePriceHistory(mockJsonResponse, false)).thenReturn(emptyList())

        // Act
        val result = forexService.fetchPriceHistory("EUR/USD", 30)

        // Assert
        assertEquals(0, result.size)
    }
}

