package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriBuilder
import reactor.core.publisher.Mono
import java.net.URI
import java.util.function.Function

class CryptoServiceTest {

    private lateinit var cryptoService: CryptoService
    private lateinit var webClientBuilder: WebClient.Builder
    private lateinit var webClient: WebClient
    private lateinit var jdbcTemplate: JdbcTemplate
    private val cryptoCompareBaseUrl = "http://mock-url.com"
    private val apiKey = "mock-api-key"

    @BeforeEach
    fun setUp() {
        webClientBuilder = Mockito.mock(WebClient.Builder::class.java)
        webClient = Mockito.mock(WebClient::class.java)
        jdbcTemplate = Mockito.mock(JdbcTemplate::class.java)

        val requestHeadersUriSpec = Mockito.mock(WebClient.RequestHeadersUriSpec::class.java)
        val requestHeadersSpec = Mockito.mock(WebClient.RequestHeadersSpec::class.java)
        val responseSpec = Mockito.mock(WebClient.ResponseSpec::class.java)

        Mockito.`when`(webClientBuilder.baseUrl(cryptoCompareBaseUrl)).thenReturn(webClientBuilder)
        Mockito.`when`(webClientBuilder.build()).thenReturn(webClient)
        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())).thenReturn(requestHeadersSpec)
        Mockito.`when`(requestHeadersSpec.retrieve()).thenReturn(responseSpec)
        Mockito.`when`(responseSpec.bodyToMono(String::class.java)).thenReturn(
            Mono.just("""{"USD": 12345.67}""")
        )

        cryptoService = CryptoService(webClientBuilder, cryptoCompareBaseUrl, apiKey, ObjectMapper(), jdbcTemplate)
    }

    @Test
    fun `should fetch current price successfully`() {
        val result = cryptoService.fetchCryptoPrice("BTC", "USD")
        Assertions.assertNotNull(result)
        Assertions.assertEquals(12345.67, result?.get("USD")?.asDouble())
    }

    @Test
    fun `should store historical data successfully`() {
        // Mock the API response with historical data
        val historicalData = """
        {
            "Data": {
                "Data": [
                    {"time": 1234567890, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volumefrom": 100.0, "volumeto": 150.0}
                ]
            }
        }
    """
        val requestHeadersUriSpec = Mockito.mock(WebClient.RequestHeadersUriSpec::class.java)
        val requestHeadersSpec = Mockito.mock(WebClient.RequestHeadersSpec::class.java)
        val responseSpec = Mockito.mock(WebClient.ResponseSpec::class.java)

        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())).thenReturn(requestHeadersSpec)
        Mockito.`when`(requestHeadersSpec.retrieve()).thenReturn(responseSpec)
        Mockito.`when`(responseSpec.bodyToMono(String::class.java)).thenReturn(Mono.just(historicalData))

        // Mock the database operation
        Mockito.`when`(
            jdbcTemplate.update(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(1)

        // Call the method under test
        cryptoService.fetchAndStoreHistoricalData("BTC", "USD", 10)

        // Verify the database insertion
        Mockito.verify(jdbcTemplate, Mockito.times(1)).update(
            Mockito.anyString(),
            Mockito.eq("BTC"),
            Mockito.eq("USD"),
            Mockito.eq(1.0),
            Mockito.eq(2.0),
            Mockito.eq(0.5),
            Mockito.eq(1.5),
            Mockito.eq(100.0),
            Mockito.eq(150.0),
            Mockito.any(),
            Mockito.eq("CryptoCompare API")
        )
    }
}