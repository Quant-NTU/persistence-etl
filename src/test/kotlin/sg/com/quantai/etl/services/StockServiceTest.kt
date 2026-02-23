package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.JsonNode
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

class StockServiceTest {

    private lateinit var stockService: StockService
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
            Mono.just("""{"values": [{"datetime": "2024-01-15", "open": "150.0", "high": "155.0", "low": "148.0", "close": "152.0", "volume": "1000000"}]}""")
        )

        // Initialize the service
        stockService = StockService(webClientBuilder, twelveDataBaseUrl, apiKey, objectMapper, jdbcTemplate, twelveDataApiClient)
    }

    @Test
    fun `should return top stock symbols`() {
        val symbols = stockService.getTopStockSymbols()
        
        assertEquals(10, symbols.size)
        assertEquals("AAPL", symbols[0])
        assertEquals("MSFT", symbols[1])
        assertEquals("GOOGL", symbols[2])
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
        stockService.fetchAndStoreHistoricalData("AAPL", 5, "1day")

        // Verify batch update was called
        Mockito.verify(jdbcTemplate, Mockito.times(1)).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `should store historical data for top symbols`() {
        // Mock batch update for the new implementation
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenReturn(intArrayOf(1)) // Successful batch insertion

        // Call the method
        stockService.storeHistoricalDataForTopSymbols()

        // Verify that batch update was called for multiple symbols (10 top symbols)
        Mockito.verify(jdbcTemplate, Mockito.atLeast(1)).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `should fetch and store stock historical data by date successfully`() {
        // Mock batch insert
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenReturn(intArrayOf(1))

        // Call method
        stockService.fetchAndStoreHistoricalDataByDate(
            symbol = "AAPL",
            interval = "1day",
            startDate = "2024-01-01",
            endDate = "2024-01-31"
        )

        // Verify DB insert happened
        Mockito.verify(jdbcTemplate, Mockito.atLeastOnce()).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `should handle exception gracefully during stock historical fetch by date`() {
        // Force DB failure
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenThrow(RuntimeException("DB error"))

        // Method handles errors gracefully by catching and logging - no exception thrown
        stockService.fetchAndStoreHistoricalDataByDate(
            symbol = "AAPL",
            interval = "1day",
            startDate = "2024-01-01",
            endDate = "2024-01-31"
        )
        
        // Verify the method completed without throwing an exception
        // (errors are logged internally)
    }

    @Test
    fun `fetchPriceHistory should return price data on success`() {
        // Arrange
        val mockJsonResponse = objectMapper.readTree("""{"values": [{"datetime": "2024-01-15", "open": 150.0, "high": 155.0, "low": 148.0, "close": 152.0, "volume": 1000000}]}""")
        val mockPriceData = listOf(
            mapOf("date" to "2024-01-15", "open" to 150.0, "high" to 155.0, "low" to 148.0, "close" to 152.0, "volume" to 1000000L)
        )
        
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("AAPL", "1day", 30)).thenReturn(mockJsonResponse)
        Mockito.`when`(twelveDataApiClient.parsePriceHistory(mockJsonResponse, true)).thenReturn(mockPriceData)

        // Act
        val result = stockService.fetchPriceHistory("AAPL", 30)

        // Assert
        assertEquals(1, result.size)
        assertEquals("2024-01-15", result[0]["date"])
        assertEquals(152.0, result[0]["close"])
        Mockito.verify(twelveDataApiClient).fetchTimeSeries("AAPL", "1day", 30)
        Mockito.verify(twelveDataApiClient).parsePriceHistory(mockJsonResponse, true)
    }

    @Test
    fun `fetchPriceHistory should throw exception when API returns null`() {
        // Arrange
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("AAPL", "1day", 30)).thenReturn(null)

        // Act & Assert
        val exception = assertThrows(RuntimeException::class.java) {
            stockService.fetchPriceHistory("AAPL", 30)
        }
        assertEquals("Failed to fetch price history for AAPL", exception.message)
    }

    @Test
    fun `fetchSP500DailyPrices should return price data on success`() {
        // Arrange
        val mockJsonResponse = objectMapper.readTree("""{"values": [{"datetime": "2024-01-15", "open": 500.0, "high": 505.0, "low": 498.0, "close": 502.0, "volume": 5000000}]}""")
        val mockPriceData = listOf(
            mapOf("date" to "2024-01-15", "open" to 500.0, "high" to 505.0, "low" to 498.0, "close" to 502.0, "volume" to 5000000L)
        )
        
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("SPY", "1day", 30)).thenReturn(mockJsonResponse)
        Mockito.`when`(twelveDataApiClient.parsePriceHistory(mockJsonResponse, true)).thenReturn(mockPriceData)

        // Act
        val result = stockService.fetchSP500DailyPrices(30)

        // Assert
        assertEquals(1, result.size)
        assertEquals("2024-01-15", result[0]["date"])
        assertEquals(502.0, result[0]["close"])
    }

    @Test
    fun `fetchSP500DailyPrices should throw exception when API returns null`() {
        // Arrange
        Mockito.`when`(twelveDataApiClient.fetchTimeSeries("SPY", "1day", 30)).thenReturn(null)

        // Act & Assert
        val exception = assertThrows(RuntimeException::class.java) {
            stockService.fetchSP500DailyPrices(30)
        }
        assertEquals("Failed to fetch S&P 500 data", exception.message)
    }

}

