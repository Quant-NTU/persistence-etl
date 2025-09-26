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

class ForexServiceTest {

    private lateinit var forexService: ForexService
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
            Mono.just("""{"values": [{"datetime": "2024-01-15", "open": "1.0850", "high": "1.0890", "low": "1.0820", "close": "1.0875"}]}""")
        )

        // Initialize the service
        forexService = ForexService(webClientBuilder, twelveDataBaseUrl, apiKey, ObjectMapper(), jdbcTemplate)
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
                Mockito.any()
            )
        ).thenReturn(1) // Successful insertion

        // Call the method
        forexService.fetchAndStoreHistoricalData("EUR/USD", 5)

        // Verify database interactions
        Mockito.verify(jdbcTemplate, Mockito.times(1)).queryForObject(
            Mockito.anyString(),
            Mockito.eq(Int::class.java),
            Mockito.eq("EUR/USD"),
            Mockito.any()
        )

        Mockito.verify(jdbcTemplate, Mockito.times(1)).update(
            Mockito.anyString(),
            Mockito.eq("EUR/USD"),
            Mockito.eq(1.0850),
            Mockito.eq(1.0890),
            Mockito.eq(1.0820),
            Mockito.eq(1.0875),
            Mockito.any()
        )
    }

    @Test
    fun `should store historical data for top pairs`() {
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
                Mockito.any()
            )
        ).thenReturn(1) // Successful insertion

        // Call the method
        forexService.storeHistoricalDataForTopPairs()

        // Verify that database operations were called for multiple pairs
        Mockito.verify(jdbcTemplate, Mockito.atLeast(1)).queryForObject(
            Mockito.anyString(),
            Mockito.eq(Int::class.java),
            Mockito.any(),
            Mockito.any()
        )
    }
}

