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
class StockService(
    private val webClientBuilder: WebClient.Builder,
    @Value("\${quantai.external.api.twelvedata.url}") private val twelveDataBaseUrl: String,
    @Value("\${quantai.external.api.twelvedata.key}") private val apiKey: String,
    private val objectMapper: ObjectMapper,
    private val jdbcTemplate: JdbcTemplate
) {

    private val logger: Logger = LoggerFactory.getLogger(StockService::class.java)

    private val webClient: WebClient by lazy {
        webClientBuilder.baseUrl(twelveDataBaseUrl).build()
    }

    fun getTopStockSymbols(): List<String> {
        // Return common US stock symbols for now - in production you might want to fetch this dynamically
        return listOf("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V")
    }

    fun storeHistoricalDataForTopSymbols() {
        val topSymbols = getTopStockSymbols()

        topSymbols.forEach { symbol ->
            try {
                val historicalData = fetchHistoricalData(symbol, 30)
                if (historicalData != null && historicalData.has("values") && historicalData["values"].isArray) {
                    historicalData["values"].forEach { node ->
                        val timestamp = parseTimestamp(node["datetime"].asText())
                        if (!checkIfDataExists(symbol, timestamp)) {
                            insertHistoricalData(node, symbol)
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

    fun fetchAndStoreHistoricalData(symbol: String, outputsize: Int) {
        val historicalData = fetchHistoricalData(symbol, outputsize)
        if (historicalData == null || !historicalData.has("values") || !historicalData["values"].isArray || historicalData["values"].isEmpty) {
            logger.warn("No historical data found for $symbol")
            return
        }

        historicalData["values"].forEach { node ->
            val timestamp = parseTimestamp(node["datetime"].asText())
            if (!checkIfDataExists(symbol, timestamp)) {
                insertHistoricalData(node, symbol)
            } else {
                logger.info("Data for $symbol at $timestamp already exists. Skipping storage.")
            }
        }
    }

    private fun fetchHistoricalData(symbol: String, outputsize: Int): JsonNode? {
        try {
            logger.info("Fetching historical data for stock $symbol")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/time_series")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", "1day")
                        .queryParam("outputsize", outputsize)
                        .queryParam("apikey", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            logger.info("API Response for stock historical data: $response")

            return objectMapper.readTree(response)
        } catch (e: Exception) {
            logger.error("Error fetching historical data for stock $symbol: ${e.message}")
            return null
        }
    }

    private fun insertHistoricalData(node: JsonNode, symbol: String) {
        try {
            val timestamp = parseTimestamp(node["datetime"].asText())
            val open = node["open"]?.asDouble() ?: throw IllegalArgumentException("Missing 'open'")
            val high = node["high"]?.asDouble() ?: throw IllegalArgumentException("Missing 'high'")
            val low = node["low"]?.asDouble() ?: throw IllegalArgumentException("Missing 'low'")
            val close = node["close"]?.asDouble() ?: throw IllegalArgumentException("Missing 'close'")
            val volume = node["volume"]?.asLong() ?: throw IllegalArgumentException("Missing 'volume'")

            logger.info(
                "Inserting: symbol=$symbol, open=$open, high=$high, low=$low, close=$close, " +
                        "volume=$volume, timestamp=$timestamp"
            )

            val sql = """
                INSERT INTO raw_stock_data (symbol, open, high, low, close, volume, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            jdbcTemplate.update(sql, symbol, open, high, low, close, volume, timestamp)
        } catch (e: Exception) {
            logger.error("Failed to insert historical data for stock $symbol: ${e.message}")
        }
    }

    private fun checkIfDataExists(symbol: String, timestamp: Timestamp): Boolean {
        val sql = """
            SELECT COUNT(*) FROM raw_stock_data WHERE symbol = ? AND timestamp = ?
        """
        val count = jdbcTemplate.queryForObject(sql, Int::class.java, symbol, timestamp)
        return count != null && count > 0
    }

    private fun parseTimestamp(datetime: String): Timestamp {
        // Twelve Data returns datetime in format "2024-01-15" for daily data
        val localDateTime = LocalDateTime.parse("${datetime}T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        return Timestamp.valueOf(localDateTime)
    }
}
