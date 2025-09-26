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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Service
class ForexService(
    private val webClientBuilder: WebClient.Builder,
    @Value("\${quantai.external.api.twelvedata.url}") private val twelveDataBaseUrl: String,
    @Value("\${quantai.external.api.twelvedata.key}") private val apiKey: String,
    private val objectMapper: ObjectMapper,
    private val jdbcTemplate: JdbcTemplate
) {

    private val logger: Logger = LoggerFactory.getLogger(ForexService::class.java)

    private val webClient: WebClient by lazy {
        webClientBuilder.baseUrl(twelveDataBaseUrl).build()
    }

    fun getTopForexPairs(): List<String> {
        // Return common forex pairs
        return listOf("EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY")
    }

    fun storeHistoricalDataForTopPairs() {
        val topPairs = getTopForexPairs()

        topPairs.forEach { pair ->
            try {
                val historicalData = fetchHistoricalData(pair, 30)
                if (historicalData != null && historicalData.has("values") && historicalData["values"].isArray) {
                    historicalData["values"].forEach { node ->
                        val timestamp = parseTimestamp(node["datetime"].asText())
                        if (!checkIfDataExists(pair, timestamp)) {
                            insertHistoricalData(node, pair)
                        } else {
                            logger.info("Data for $pair at $timestamp already exists. Skipping storage.")
                        }
                    }
                }
            } catch (e: Exception) {
                logger.error("Error storing historical data for $pair: ${e.message}")
            }
        }
    }

    fun fetchAndStoreHistoricalData(currencyPair: String, outputsize: Int) {
        val historicalData = fetchHistoricalData(currencyPair, outputsize)
        if (historicalData == null || !historicalData.has("values") || !historicalData["values"].isArray || historicalData["values"].isEmpty) {
            logger.warn("No historical data found for $currencyPair")
            return
        }

        historicalData["values"].forEach { node ->
            val timestamp = parseTimestamp(node["datetime"].asText())
            if (!checkIfDataExists(currencyPair, timestamp)) {
                insertHistoricalData(node, currencyPair)
            } else {
                logger.info("Data for $currencyPair at $timestamp already exists. Skipping storage.")
            }
        }
    }

    private fun fetchHistoricalData(currencyPair: String, outputsize: Int): JsonNode? {
        try {
            logger.info("Fetching historical data for forex pair $currencyPair")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/time_series")
                        .queryParam("symbol", currencyPair)
                        .queryParam("interval", "1day")
                        .queryParam("outputsize", outputsize)
                        .queryParam("apikey", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            logger.info("API Response for forex historical data: $response")

            return objectMapper.readTree(response)
        } catch (e: Exception) {
            logger.error("Error fetching historical data for forex pair $currencyPair: ${e.message}")
            return null
        }
    }

    private fun insertHistoricalData(node: JsonNode, currencyPair: String) {
        try {
            val timestamp = parseTimestamp(node["datetime"].asText())
            val open = node["open"]?.asDouble() ?: throw IllegalArgumentException("Missing 'open'")
            val high = node["high"]?.asDouble() ?: throw IllegalArgumentException("Missing 'high'")
            val low = node["low"]?.asDouble() ?: throw IllegalArgumentException("Missing 'low'")
            val close = node["close"]?.asDouble() ?: throw IllegalArgumentException("Missing 'close'")

            logger.info(
                "Inserting: currency_pair=$currencyPair, open=$open, high=$high, low=$low, close=$close, " +
                        "timestamp=$timestamp"
            )

            val sql = """
                INSERT INTO raw_forex_data (currency_pair, open, high, low, close, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            jdbcTemplate.update(sql, currencyPair, open, high, low, close, timestamp)
        } catch (e: Exception) {
            logger.error("Failed to insert historical data for forex pair $currencyPair: ${e.message}")
        }
    }

    private fun checkIfDataExists(currencyPair: String, timestamp: Timestamp): Boolean {
        val sql = """
            SELECT COUNT(*) FROM raw_forex_data WHERE currency_pair = ? AND timestamp = ?
        """
        val count = jdbcTemplate.queryForObject(sql, Int::class.java, currencyPair, timestamp)
        return count != null && count > 0
    }

    private fun parseTimestamp(datetime: String): Timestamp {
        // Twelve Data returns datetime in format "2024-01-15" for daily data
        val localDateTime = LocalDateTime.parse("${datetime}T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        return Timestamp.valueOf(localDateTime)
    }
}
