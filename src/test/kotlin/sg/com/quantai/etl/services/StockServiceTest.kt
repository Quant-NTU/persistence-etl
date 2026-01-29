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

class StockServiceTest {

    private lateinit var stockService: StockService
    private lateinit var webClientBuilder: WebClient.Builder
    private lateinit var webClient: WebClient
    private lateinit var jdbcTemplate: JdbcTemplate
    private val twelveDataBaseUrl = "https://api.twelvedata.com"
    private val apiKey = "test-api-key"

    @BeforeEach
    fun setUp() {
        webClientBuilder = Mockito.mock(WebClient.Builder::class.java)
        webClient = Mockito.mock(WebClient::class.java)
        jdbcTemplate = Mockito.mock(JdbcTemplate::class.java)

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
        stockService = StockService(webClientBuilder, twelveDataBaseUrl, apiKey, ObjectMapper(), jdbcTemplate)
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
            "AAPL",
            "2024-01-01",
            "2024-01-31"
        )

        // Verify DB insert happened
        Mockito.verify(jdbcTemplate, Mockito.atLeastOnce()).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `should throw exception when error occurs during stock historical fetch by date`() {
        // Force DB failure
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenThrow(RuntimeException("DB error"))

        val exception = org.junit.jupiter.api.assertThrows<RuntimeException> {
            stockService.fetchAndStoreHistoricalDataByDate(
                "AAPL",
                "2024-01-01",
                "2024-01-31"
            )
        }

        assertEquals("DB error", exception.message)
    }

}

