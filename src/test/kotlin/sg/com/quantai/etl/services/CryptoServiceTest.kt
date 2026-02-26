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

        // WebClient mock setup
        Mockito.`when`(webClientBuilder.baseUrl(cryptoCompareBaseUrl)).thenReturn(webClientBuilder)
        Mockito.`when`(webClientBuilder.build()).thenReturn(webClient)
        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(
            requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())
        ).thenReturn(requestHeadersSpec)
        Mockito.`when`(requestHeadersSpec.retrieve()).thenReturn(responseSpec)

        // Mock fetchHistoricalData response
        Mockito.`when`(responseSpec.bodyToMono(String::class.java)).thenReturn(
            Mono.just("""{"Data": {"Data": [{"time": 1234567890, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volumefrom": 100.0, "volumeto": 150.0}]}}""")
        )

        // Initialize the service
        cryptoService = CryptoService(webClientBuilder, cryptoCompareBaseUrl, apiKey, ObjectMapper(), jdbcTemplate)
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
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(1) // Successful insertion

        // Call the method
        cryptoService.fetchAndStoreHistoricalData("BTC", "USD", 10)

        // Verify database interactions
        Mockito.verify(jdbcTemplate, Mockito.times(1)).queryForObject(
            Mockito.anyString(),
            Mockito.eq(Int::class.java),
            Mockito.eq("BTC"),
            Mockito.any()
        )

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
            Mockito.any()
        )
    }

    @Test
    fun `should store historical data for top symbols`() {
        // Mock top symbols
        Mockito.`when`(jdbcTemplate.update(Mockito.anyString())).thenReturn(5)

        Mockito.`when`(jdbcTemplate.update(Mockito.anyString())).thenReturn(1)
        Mockito.`when`(jdbcTemplate.update(Mockito.anyString())).thenReturn(1)
    }

    @Test
    fun `should fetch and store historical data by date successfully`() {
        // Mock database call: no data exists yet for that timestamp
        Mockito.`when`(
            jdbcTemplate.queryForObject(
                Mockito.anyString(),
                Mockito.eq(Int::class.java),
                Mockito.any(),
                Mockito.any()
            )
        ).thenReturn(0) // no existing data

        // Mock insert successful
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
                Mockito.any()
            )
        ).thenReturn(1)

        // Call method with date range
        cryptoService.fetchAndStoreHistoricalDataByDate("BTC", "USD", "2025-01-01", "2025-01-02")

        // Verify interactions with DB
        Mockito.verify(jdbcTemplate, Mockito.atLeastOnce()).queryForObject(
            Mockito.anyString(),
            Mockito.eq(Int::class.java),
            Mockito.eq("BTC"),
            Mockito.any()
        )
        Mockito.verify(jdbcTemplate, Mockito.atLeastOnce()).update(
            Mockito.anyString(),
            Mockito.eq("BTC"),
            Mockito.eq("USD"),
            Mockito.eq(1.0),
            Mockito.eq(2.0),
            Mockito.eq(0.5),
            Mockito.eq(1.5),
            Mockito.eq(100.0),
            Mockito.eq(150.0),
            Mockito.any()
        )
    }

    @Test
    fun `fetchPriceHistory should return price data on success`() {
        // The mocked API response already set up in setUp() will be used
        // It returns: {"Data": {"Data": [{"time": 1234567890, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volumefrom": 100.0, "volumeto": 150.0}]}}
        
        // Act
        val result = cryptoService.fetchPriceHistory("BTC", "USD", 30)

        // Assert
        org.junit.jupiter.api.Assertions.assertEquals(1, result.size)
        org.junit.jupiter.api.Assertions.assertEquals(1.0, result[0]["open"])
        org.junit.jupiter.api.Assertions.assertEquals(2.0, result[0]["high"])
        org.junit.jupiter.api.Assertions.assertEquals(0.5, result[0]["low"])
        org.junit.jupiter.api.Assertions.assertEquals(1.5, result[0]["close"])
        org.junit.jupiter.api.Assertions.assertEquals(100.0, result[0]["volumeFrom"])
        org.junit.jupiter.api.Assertions.assertEquals(150.0, result[0]["volumeTo"])
    }

    @Test
    fun `fetchPriceHistory should return empty list when no data available`() {
        // Arrange - Override the mock to return empty data
        val requestHeadersUriSpec = Mockito.mock(WebClient.RequestHeadersUriSpec::class.java)
        val requestHeadersSpec = Mockito.mock(WebClient.RequestHeadersSpec::class.java)
        val responseSpec = Mockito.mock(WebClient.ResponseSpec::class.java)

        Mockito.`when`(webClient.get()).thenReturn(requestHeadersUriSpec)
        Mockito.`when`(
            requestHeadersUriSpec.uri(Mockito.any<Function<UriBuilder, URI>>())
        ).thenReturn(requestHeadersSpec)
        Mockito.`when`(requestHeadersSpec.retrieve()).thenReturn(responseSpec)
        Mockito.`when`(responseSpec.bodyToMono(String::class.java)).thenReturn(
            Mono.just("""{"Data": {"Data": []}}""")
        )

        // Act
        val result = cryptoService.fetchPriceHistory("BTC", "USD", 30)

        // Assert
        org.junit.jupiter.api.Assertions.assertEquals(0, result.size)
    }

}