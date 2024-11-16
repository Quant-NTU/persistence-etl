package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import java.sql.Timestamp
import java.time.Instant

@Service
class CryptoService(
    private val restTemplate: RestTemplate,
    @Value("\${CRYPTOCOMPARE_API_KEY}") private val apiKey: String,
    private val objectMapper: ObjectMapper,
    private val jdbcTemplate: JdbcTemplate
) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoService::class.java)

    // Fetch the current price of a cryptocurrency
    fun fetchCryptoPrice(symbol: String, currency: String): JsonNode {
        val url = "https://min-api.cryptocompare.com/data/price?fsym=${symbol}&tsyms=${currency}&api_key=${apiKey}"
        logger.info("Fetching current price for $symbol in $currency from URL: $url")
        val response = restTemplate.getForEntity(url, String::class.java)
        return objectMapper.readTree(response.body)
    }

    // Fetch historical data for a cryptocurrency
    fun fetchHistoricalData(symbol: String, currency: String, limit: Int): JsonNode {
        val url = "https://min-api.cryptocompare.com/data/v2/histoday?fsym=${symbol}&tsym=${currency}&limit=${limit}&api_key=${apiKey}"
        logger.info("Fetching historical data for $symbol in $currency from URL: $url")
        val response = restTemplate.getForEntity(url, String::class.java)
        return objectMapper.readTree(response.body)
    }

    // Fetch and store historical data into the database
    fun fetchAndStoreHistoricalData(symbol: String, currency: String, limit: Int) {
        val source = "CryptoCompare API" // Set the source to "CryptoCompare API"
        val url = "https://min-api.cryptocompare.com/data/v2/histoday?fsym=$symbol&tsym=$currency&limit=$limit&api_key=$apiKey"
        logger.info("Fetching historical data for $symbol in $currency with source $source from URL: $url")

        try {
            val response = restTemplate.getForEntity(url, String::class.java)
            val data = objectMapper.readTree(response.body).path("Data").path("Data")

            if (data.isEmpty) {
                logger.warn("No historical data found for $symbol in $currency with source $source")
                return
            }

            logger.info("Fetched ${data.size()} records for $symbol-$currency")

            // Insert data into the database
            data.forEach { node ->
                try {
                    // Extract values from the API response
                    val timestamp = Timestamp.from(Instant.ofEpochSecond(node["time"].asLong()))
                    val open = node["open"]?.asDouble() ?: throw IllegalArgumentException("Missing 'open'")
                    val high = node["high"]?.asDouble() ?: throw IllegalArgumentException("Missing 'high'")
                    val low = node["low"]?.asDouble() ?: throw IllegalArgumentException("Missing 'low'")
                    val close = node["close"]?.asDouble() ?: throw IllegalArgumentException("Missing 'close'")
                    val volumeFrom = node["volumefrom"]?.asDouble() ?: throw IllegalArgumentException("Missing 'volumefrom'")
                    val volumeTo = node["volumeto"]?.asDouble() ?: throw IllegalArgumentException("Missing 'volumeto'")

                    // Log the data being inserted
                    logger.info(
                        "Inserting: symbol=$symbol, currency=$currency, open=$open, high=$high, low=$low, close=$close, " +
                                "volume_from=$volumeFrom, volume_to=$volumeTo, timestamp=$timestamp, source=$source"
                    )

                    // Execute the SQL insertion
                    val sql = """
                        INSERT INTO raw_crypto_data (symbol, currency, open, high, low, close, volume_from, volume_to, timestamp, "source")
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    jdbcTemplate.update(sql, symbol, currency, open, high, low, close, volumeFrom, volumeTo, timestamp, source)
                } catch (e: Exception) {
                    // Log errors for debugging
                    logger.error("Failed to insert historical data for $symbol-$currency: ${e.message}")
                }
            }
        } catch (e: Exception) {
            // Log API or unexpected errors
            logger.error("Error fetching historical data for $symbol-$currency: ${e.message}")
        }
    }
}
