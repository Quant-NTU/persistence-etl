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
    fun `should fetch and store forex historical data by date successfully`() {
        // Arrange
        Mockito.`when`(
            jdbcTemplate.batchUpdate(
                Mockito.anyString(),
                Mockito.anyList()
            )
        ).thenReturn(intArrayOf(1))

        // Act
        forexService.fetchAndStoreHistoricalDataByDate(
            currencyPair = "EUR/USD",
            interval = "1day",
            startDate = "2024-01-01",
            endDate = "2024-01-31"
        )

        // Assert
        Mockito.verify(jdbcTemplate, Mockito.times(1)).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

    @Test
    fun `should not store forex data by date when api returns no values`() {
        // Arrange — override API response to return empty JSON
        Mockito.`when`(
            webClient.get()
                .uri(Mockito.any<Function<UriBuilder, URI>>())
                .retrieve()
                .bodyToMono(String::class.java)
        ).thenReturn(Mono.just("{}"))

        // Act
        forexService.fetchAndStoreHistoricalDataByDate(
            currencyPair = "EUR/USD",
            interval = "1day",
            startDate = "2024-01-01",
            endDate = "2024-01-31"
        )

        // Assert
        Mockito.verify(jdbcTemplate, Mockito.never()).batchUpdate(
            Mockito.anyString(),
            Mockito.anyList()
        )
    }

}

