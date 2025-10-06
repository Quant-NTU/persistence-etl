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
        // Mock database query and update
        Mockito.`when`(
            jdbcTemplate.queryForObject(
                Mockito.anyString(),
                Mockito.eq(Int::class.java),
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(0) // No existing data

        Mockito.`when`(
            jdbcTemplate.update(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(1) // Successful insertion

        // Call the method
        stockService.fetchAndStoreHistoricalData("AAPL", 5, "1day")

        // Verify database interactions
        Mockito.verify(jdbcTemplate, Mockito.times(1)).queryForObject(
            Mockito.anyString(),
            Mockito.eq(Int::class.java),
            Mockito.eq("AAPL"),
            Mockito.any()
        )

        Mockito.verify(jdbcTemplate, Mockito.times(1)).update(
            Mockito.anyString(),
            Mockito.eq("AAPL"),
            Mockito.eq(150.0),
            Mockito.eq(155.0),
            Mockito.eq(148.0),
            Mockito.eq(152.0),
            Mockito.eq(1000000L),
            Mockito.any()
        )
    }

    @Test
    fun `should store historical data for top symbols`() {
        // Mock database operations
        Mockito.`when`(
            jdbcTemplate.queryForObject(
                Mockito.anyString(),
                Mockito.eq(Int::class.java),
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(0) // No existing data

        Mockito.`when`(
            jdbcTemplate.update(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(1) // Successful insertion

        // Call the method
        stockService.storeHistoricalDataForTopSymbols()

        // Verify that database operations were called for multiple symbols
        Mockito.verify(jdbcTemplate, Mockito.atLeast(1)).queryForObject(
            Mockito.anyString(),
            Mockito.eq(Int::class.java),
            Mockito.any(),
            Mockito.any()
        )
    }
}

