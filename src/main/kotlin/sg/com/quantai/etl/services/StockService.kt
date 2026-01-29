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
import java.time.LocalDate
import java.time.LocalTime


data class StockDataRecord(
    val symbol: String,
    val interval: String,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val volume: Long,
    val startDateTime: Timestamp,
    val endDateTime: Timestamp,
    val timestamp: Timestamp
)

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
                fetchAndStoreHistoricalData(symbol, 30, "1day")
            } catch (e: Exception) {
                logger.error("Error storing historical data for $symbol: ${e.message}")
            }
        }
    }

    fun fetchAndStoreHistoricalData(symbol: String, limit: Int, interval: String = "1day") {
        val historicalData = fetchHistoricalData(symbol, limit, interval)
        if (historicalData == null || !historicalData.has("values") || !historicalData["values"].isArray || historicalData["values"].isEmpty) {
            logger.warn("No historical data found for $symbol with interval $interval")
            return
        }

        // Collect all data for batch processing
        val batchData = mutableListOf<StockDataRecord>()
        
        historicalData["values"].forEach { node ->
            try {
                val timestamp = parseTimestamp(node["datetime"].asText(), interval)
                val open = node["open"]?.asDouble() ?: throw IllegalArgumentException("Missing 'open'")
                val high = node["high"]?.asDouble() ?: throw IllegalArgumentException("Missing 'high'")
                val low = node["low"]?.asDouble() ?: throw IllegalArgumentException("Missing 'low'")
                val close = node["close"]?.asDouble() ?: throw IllegalArgumentException("Missing 'close'")
                val volume = node["volume"]?.asLong() ?: throw IllegalArgumentException("Missing 'volume'")
                val (startDateTime, endDateTime) = calculateKLineInterval(timestamp, interval)
                
                batchData.add(StockDataRecord(symbol, interval, open, high, low, close, volume, startDateTime, endDateTime, timestamp))
            } catch (e: Exception) {
                logger.error("Error parsing data for $symbol: ${e.message}")
            }
        }
        
        if (batchData.isNotEmpty()) {
            batchInsertHistoricalData(batchData)
        }
    }

    fun getSupportedIntervals(): List<String> {
        return listOf("1min", "5min", "15min", "1h", "4h", "1day")
    }

    private fun fetchHistoricalData(symbol: String, outputsize: Int, interval: String = "1day"): JsonNode? {
        try {
            logger.info("Fetching historical data for stock $symbol with interval $interval")

            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/time_series")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
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
            logger.error("Error fetching historical data for stock $symbol with interval $interval: ${e.message}")
            return null
        }
    }

    

    private fun batchInsertHistoricalData(batchData: List<StockDataRecord>) {
        try {
            logger.info("Batch inserting ${batchData.size} stock records")
            
            val sql = """
                INSERT INTO raw_stock_data (symbol, interval, open, high, low, close, volume, start_date_time, end_date_time, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, interval, timestamp) DO NOTHING
            """
            
            val batchArgs = batchData.map { record ->
                arrayOf(
                    record.symbol,
                    record.interval,
                    record.open,
                    record.high,
                    record.low,
                    record.close,
                    record.volume,
                    record.startDateTime,
                    record.endDateTime,
                    record.timestamp
                )
            }
            jdbcTemplate.batchUpdate(sql, batchArgs)
            
            logger.info("Successfully batch inserted ${batchData.size} stock records")
        } catch (e: Exception) {
            logger.error("Failed to batch insert stock data: ${e.message}")
            // Fallback to individual inserts for debugging
            batchData.forEach { record ->
                try {
                    insertSingleRecord(record)
                } catch (ex: Exception) {
                    logger.error("Failed to insert individual record for ${record.symbol}: ${ex.message}")
                }
            }
        }
    }

    private fun insertSingleRecord(record: StockDataRecord) {
        val sql = """
            INSERT INTO raw_stock_data (symbol, interval, open, high, low, close, volume, start_date_time, end_date_time, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, interval, timestamp) DO NOTHING
        """
        jdbcTemplate.update(sql, record.symbol, record.interval, record.open, record.high, 
                          record.low, record.close, record.volume, record.startDateTime, record.endDateTime, record.timestamp)
    }

    private fun parseTimestamp(datetime: String, interval: String = "1day"): Timestamp {
        return when {
            interval.contains("min") || interval.contains("h") -> {
                // For intraday data: "2024-01-15 09:30:00"
                val localDateTime = LocalDateTime.parse(datetime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                Timestamp.valueOf(localDateTime)
            }
            else -> {
                // For daily data: "2024-01-15"
                val localDateTime = LocalDateTime.parse("${datetime}T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                Timestamp.valueOf(localDateTime)
            }
        }
    }

    private fun calculateKLineInterval(timestamp: Timestamp, interval: String): Pair<Timestamp, Timestamp> {
        val localDateTime = timestamp.toLocalDateTime()
        
        return when (interval) {
            "1min" -> {
                val start = localDateTime.withSecond(0).withNano(0)
                val end = start.plusMinutes(1).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
            "5min" -> {
                val minute = localDateTime.minute
                val roundedMinute = (minute / 5) * 5
                val start = localDateTime.withMinute(roundedMinute).withSecond(0).withNano(0)
                val end = start.plusMinutes(5).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
            "15min" -> {
                val minute = localDateTime.minute
                val roundedMinute = (minute / 15) * 15
                val start = localDateTime.withMinute(roundedMinute).withSecond(0).withNano(0)
                val end = start.plusMinutes(15).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
            "1h" -> {
                val start = localDateTime.withMinute(0).withSecond(0).withNano(0)
                val end = start.plusHours(1).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
            "4h" -> {
                val hour = localDateTime.hour
                val roundedHour = (hour / 4) * 4
                val start = localDateTime.withHour(roundedHour).withMinute(0).withSecond(0).withNano(0)
                val end = start.plusHours(4).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
            "1day" -> {
                val start = localDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0)
                val end = start.plusDays(1).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
            else -> {
                // Default to daily
                val start = localDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0)
                val end = start.plusDays(1).minusNanos(1)
                Pair(Timestamp.valueOf(start), Timestamp.valueOf(end))
            }
        }
    }

    private fun fetchHistoricalDataByDate(
        symbol: String,
        interval: String,
        startDate: LocalDate,
        endDate: LocalDate
    ): JsonNode? {

        return try {
            val response = webClient
                .get()
                .uri { uriBuilder ->
                    uriBuilder
                        .path("/time_series")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", interval)
                        .queryParam("start_date", formatDateForApi(startDate, interval, isStart = true))
                        .queryParam("end_date", formatDateForApi(endDate, interval, isStart = false))
                        .queryParam("apikey", apiKey)
                        .build()
                }
                .retrieve()
                .bodyToMono(String::class.java)
                .block()

            objectMapper.readTree(response)

        } catch (e: Exception) {
            logger.error("Error fetching historical data: ${e.message}")
            null
        }
    }


    fun fetchAndStoreHistoricalDataByDate(
        symbol: String,
        interval: String,
        startDate: String,
        endDate: String
    ) {
        try {
            logger.info("Fetching $interval data for $symbol between $startDate and $endDate")

            val start = LocalDate.parse(startDate)
            val end = LocalDate.parse(endDate)

            val historicalData = fetchHistoricalDataByDate(
                symbol = symbol,
                interval = interval,
                startDate = start,
                endDate = end
            )

            if (historicalData == null || !historicalData.has("values")) {
                logger.warn("No data returned for $symbol")
                return
            }

            val batchData = mutableListOf<StockDataRecord>()

            historicalData["values"].forEach { node ->
                val timestamp = parseTimestamp(node["datetime"].asText(), interval)
                val (startDt, endDt) = calculateKLineInterval(timestamp, interval)

                batchData.add(
                    StockDataRecord(
                        symbol = symbol,
                        interval = interval,
                        open = node["open"].asDouble(),
                        high = node["high"].asDouble(),
                        low = node["low"].asDouble(),
                        close = node["close"].asDouble(),
                        volume = node["volume"].asLong(),
                        startDateTime = startDt,
                        endDateTime = endDt,
                        timestamp = timestamp
                    )
                )
            }

            batchInsertHistoricalData(batchData)

        } catch (e: Exception) {
            logger.error("Failed fetching data for $symbol: ${e.message}")
        }
    }


    private fun formatDateForApi(
        date: LocalDate,
        interval: String,
        isStart: Boolean
    ): String {

        return if (interval.contains("min") || interval.contains("h")) {
            // Intraday requires datetime
            val time = if (isStart) LocalTime.MIN else LocalTime.MAX
            LocalDateTime.of(date, time)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        } else {
            // Daily and above
            date.format(DateTimeFormatter.ISO_LOCAL_DATE)
        }
    }


}
