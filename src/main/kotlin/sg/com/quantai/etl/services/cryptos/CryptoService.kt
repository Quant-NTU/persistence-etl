package sg.com.quantai.etl.services.cryptos

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

    fun getTopCryptoSymbols(): List<String> {
        try {
            logger.info("Fetching top 10 crypto symbols by volume")
            val apiUrl = "$cryptoCompareBaseUrl/data/top/totalvolfull?limit=10&tsym=USD&api_key=$apiKey"

            val response = webClient
                .get()
                .uri(apiUrl)
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            val responseData = objectMapper.readTree(response).path("Data")

            return responseData.map {
                it.path("CoinInfo").path("Name").asText()
            }
        } catch (e: Exception) {
            logger.error("Error fetching top crypto symbols: ${e.message}")
            return emptyList()
        }
    }

    fun storeHistoricalDataForTopSymbols() {
        val topSymbols = getTopCryptoSymbols()

        topSymbols.forEach { symbol ->
            try {
                val historicalData = fetchHistoricalData(symbol, "USD", 10)
                if (historicalData != null && historicalData.isArray) {
                    historicalData.forEach { node ->
                        val timestamp = Timestamp.from(Instant.ofEpochSecond(node["time"].asLong()))
                        if (!checkIfDataExists(symbol, timestamp)) {
                            insertHistoricalData(node, symbol, "USD")
                        } else {
                            logger.info("Data for $symbol at $timestamp already exists. Skipping storage.")
                        }
                    }
                }
            } catch (e: Exception) {
                logger.error("Error storing historical data for $symbol: ${e.message}")
            }
        }
    }

    fun fetchAndStoreHistoricalData(symbol: String, currency: String, limit: Int) {
        val historicalData = fetchHistoricalData(symbol, currency, limit)
        if (historicalData == null || !historicalData.isArray || historicalData.isEmpty) {
            logger.warn("No historical data found for $symbol in $currency")
            return
        }

        historicalData.forEach { node ->
            val timestamp = Timestamp.from(Instant.ofEpochSecond(node["time"].asLong()))
            if (!checkIfDataExists(symbol, timestamp)) {
                insertHistoricalData(node, symbol, currency)
            } else {
                logger.info("Data for $symbol at $timestamp already exists. Skipping storage.")
            }
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

    private fun insertHistoricalData(node: JsonNode, symbol: String, currency: String) {
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
                        "volume_from=$volumeFrom, volume_to=$volumeTo, timestamp=$timestamp"
            )

            val sql = """
                INSERT INTO raw_crypto_compare_crypto_data (symbol, currency, open, high, low, close, volume_from, volume_to, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            jdbcTemplate.update(sql, symbol, currency, open, high, low, close, volumeFrom, volumeTo, timestamp)
        } catch (e: Exception) {
            logger.error("Failed to insert historical data for $symbol-$currency: ${e.message}")
        }
    }

    private fun checkIfDataExists(symbol: String, timestamp: Timestamp): Boolean {
        val sql = """
            SELECT COUNT(*) FROM raw_crypto_compare_crypto_data WHERE symbol = ? AND timestamp = ?
        """
        val count = jdbcTemplate.queryForObject(sql, Int::class.java, symbol, timestamp)
        return count != null && count > 0
    }
}