package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient

/**
 * Shared utility for Twelve Data API calls.
 * Used by both StockService and ForexService to reduce code duplication.
 */
@Component
class TwelveDataApiClient(
    private val webClientBuilder: WebClient.Builder,
    @Value("\${quantai.external.api.twelvedata.url}") private val baseUrl: String,
    @Value("\${quantai.external.api.twelvedata.key}") private val apiKey: String,
    private val objectMapper: ObjectMapper
) {
    private val logger: Logger = LoggerFactory.getLogger(TwelveDataApiClient::class.java)

    private val webClient: WebClient by lazy {
        webClientBuilder.baseUrl(baseUrl).build()
    }

    /**
     * Fetch time series data from Twelve Data API.
     * 
     * @param symbol The ticker symbol (e.g., "AAPL", "EUR/USD")
     * @param interval The time interval (e.g., "1day", "1h")
     * @param outputSize Number of data points to fetch
     * @return JsonNode containing the API response, or null on error
     */
    fun fetchTimeSeries(symbol: String, interval: String, outputSize: Int): JsonNode? {
        return try {
            logger.info("Fetching time series for $symbol with interval $interval, outputSize $outputSize")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/time_series")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
                        .queryParam("outputsize", outputSize)
                        .queryParam("apikey", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            logger.debug("API Response for $symbol: $response")
            objectMapper.readTree(response)
        } catch (e: Exception) {
            logger.error("Error fetching time series for $symbol: ${e.message}")
            null
        }
    }

    /**
     * Fetch time series data with date range from Twelve Data API.
     * 
     * @param symbol The ticker symbol
     * @param interval The time interval
     * @param startDate Start date in API format
     * @param endDate End date in API format
     * @return JsonNode containing the API response, or null on error
     */
    fun fetchTimeSeriesByDateRange(
        symbol: String,
        interval: String,
        startDate: String,
        endDate: String
    ): JsonNode? {
        return try {
            logger.info("Fetching time series for $symbol from $startDate to $endDate")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/time_series")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
                        .queryParam("start_date", startDate)
                        .queryParam("end_date", endDate)
                        .queryParam("apikey", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            objectMapper.readTree(response)
        } catch (e: Exception) {
            logger.error("Error fetching time series by date range for $symbol: ${e.message}")
            null
        }
    }

    /**
     * Parse price history data from Twelve Data API response.
     * Extracts OHLC data and optionally volume.
     * 
     * @param jsonResponse The API response JSON
     * @param includeVolume Whether to include volume in the output
     * @return List of price data maps
     */
    fun parsePriceHistory(jsonResponse: JsonNode?, includeVolume: Boolean = false): List<Map<String, Any>> {
        if (jsonResponse == null || !jsonResponse.has("values") || !jsonResponse["values"].isArray) {
            logger.warn("No valid price data in API response")
            return emptyList()
        }

        val priceData = mutableListOf<Map<String, Any>>()

        jsonResponse["values"].forEach { node ->
            try {
                val dataPoint = mutableMapOf<String, Any>(
                    "date" to node["datetime"].asText(),
                    "open" to node["open"].asDouble(),
                    "high" to node["high"].asDouble(),
                    "low" to node["low"].asDouble(),
                    "close" to node["close"].asDouble()
                )
                
                if (includeVolume && node.has("volume")) {
                    dataPoint["volume"] = node["volume"].asLong()
                }
                
                priceData.add(dataPoint)
            } catch (e: Exception) {
                logger.error("Error parsing price data point: ${e.message}")
            }
        }

        return priceData
    }
}
