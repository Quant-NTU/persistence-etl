package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import java.sql.Timestamp
import java.time.Instant

@Service
class CryptoService(
    private val webClientBuilder: WebClient.Builder,
    @Value("\${quantai.external.api.cryptocompare.url}") private val cryptoCompareBaseUrl: String,
    @Value("\${quantai.external.api.cryptocompare.key}") private val apiKey: String,
    private val objectMapper: ObjectMapper,
    private val jdbcTemplate: JdbcTemplate
) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoService::class.java)

    private val webClient: WebClient by lazy {
        webClientBuilder.baseUrl(cryptoCompareBaseUrl).build()
    }

    fun fetchCryptoPrice(symbol: String, currency: String): JsonNode? {
        try {
            logger.info("Fetching current price for $symbol in $currency")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/data/price")
                        .queryParam("fsym", symbol)
                        .queryParam("tsyms", currency)
                        .queryParam("api_key", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            logger.info("API Response for current price: $response")

            return objectMapper.readTree(response)
        } catch (e: Exception) {
            logger.error("Error fetching current price for $symbol-$currency: ${e.message}")
            return null
        }
    }

    private fun fetchHistoricalData(symbol: String, currency: String, limit: Int): JsonNode? {
        try {
            logger.info("Fetching historical data for $symbol in $currency")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/data/v2/histoday")
                        .queryParam("fsym", symbol)
                        .queryParam("tsym", currency)
                        .queryParam("limit", limit)
                        .queryParam("api_key", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            logger.info("API Response for historical data: $response")

            return objectMapper.readTree(response)?.path("Data")?.path("Data")
        } catch (e: Exception) {
            logger.error("Error fetching historical data for $symbol-$currency: ${e.message}")
            return null
        }
    }

    fun fetchAndStoreHistoricalData(symbol: String, currency: String, limit: Int) {
        val source = "CryptoCompare API"
        val historicalData = fetchHistoricalData(symbol, currency, limit)

        if (historicalData == null || !historicalData.isArray || historicalData.isEmpty) {
            logger.warn("No historical data found for $symbol in $currency")
            return
        }

        logger.info("Fetched ${historicalData.size()} records for $symbol-$currency")

        historicalData.forEach { node ->
            try {
                val timestamp = Timestamp.from(Instant.ofEpochSecond(node["time"].asLong()))
                val open = node["open"]?.asDouble() ?: throw IllegalArgumentException("Missing 'open'")
                val high = node["high"]?.asDouble() ?: throw IllegalArgumentException("Missing 'high'")
                val low = node["low"]?.asDouble() ?: throw IllegalArgumentException("Missing 'low'")
                val close = node["close"]?.asDouble() ?: throw IllegalArgumentException("Missing 'close'")
                val volumeFrom = node["volumefrom"]?.asDouble() ?: throw IllegalArgumentException("Missing 'volumefrom'")
                val volumeTo = node["volumeto"]?.asDouble() ?: throw IllegalArgumentException("Missing 'volumeto'")

                logger.info(
                    "Inserting: symbol=$symbol, currency=$currency, open=$open, high=$high, low=$low, close=$close, " +
                            "volume_from=$volumeFrom, volume_to=$volumeTo, timestamp=$timestamp, source=$source"
                )

                val sql = """
                    INSERT INTO raw_crypto_data (symbol, currency, open, high, low, close, volume_from, volume_to, timestamp, "source")
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                jdbcTemplate.update(sql, symbol, currency, open, high, low, close, volumeFrom, volumeTo, timestamp, source)
            } catch (e: Exception) {
                logger.error("Failed to insert historical data for $symbol-$currency: ${e.message}")
            }
        }
    }
}