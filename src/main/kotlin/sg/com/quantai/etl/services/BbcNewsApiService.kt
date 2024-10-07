package sg.com.quantai.etl.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import sg.com.quantai.etl.data.BbcRawNewsArticle
import sg.com.quantai.etl.repositories.BbcRawNewsRepository
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.http.MediaType
import java.time.LocalDate
import java.time.YearMonth
import reactor.util.retry.Retry
import java.time.Duration


//  BBCNewsApiService handles API requests and data insertion into MongoDB.
@Service
class BbcNewsApiService(
    @Autowired private val bbcRawNewsRepository: BbcRawNewsRepository
) {

    private val newApiUrl = "https://datasets-server.huggingface.co/"
    private val batchSize = 100 // Number of rows to fetch per request

    // WebClient configuration
    private fun bbcNewsWebClient(): WebClient {
        return WebClient.builder().baseUrl(newApiUrl)
            .codecs { it.defaultCodecs().maxInMemorySize(16 * 1024 * 1024) } // Increase buffer size to 16MB
            .build()
    }

    // Fetch and save all articles from the API for multiple months
    fun fetchAndSaveNewsArticlesForAllMonths() {
        println("Loading of BBC News Articles - START")

        // Define the date range: '2017-01' to '2024-09'
        val startDate = YearMonth.of(2017, 1)
        val endDate = YearMonth.of(2024, 9)

        // Iterate over each month in the date range
        var currentMonth = startDate
        while (currentMonth <= endDate) {
            val configDate = currentMonth.toString() // Format: 'YYYY-MM'
            println("Fetching data for: $configDate")
            fetchAndSaveNewsArticlesForDate(configDate)
            currentMonth = currentMonth.plusMonths(1) // Move to the next month
        }
        println("Loading of BBC News Articles - END")
    }

    // Fetch and save all articles for a specific date (month)
    private fun fetchAndSaveNewsArticlesForDate(config: String) {
        try {
            // Step 1: Fetch the first batch to get the total number of rows
            val initialResponse = fetchRows(0, batchSize, config)
            val rootNode: JsonNode = parseResponse(initialResponse)
            val totalRows = rootNode.path("num_rows_total").asInt()

            // Process the first batch of rows
            processRows(rootNode.path("rows"))

            // Step 2: Fetch remaining rows in batches of 100
            for (offset in batchSize until totalRows step batchSize) {
                val response = fetchRows(offset, batchSize, config)
                val nextRootNode = parseResponse(response)
                processRows(nextRootNode.path("rows"))
            }

            println("Successfully saved all articles for $config.")
        } catch (ex: Exception) {
            // Handle errors gracefully and continue with the next month
            println("Error fetching data for $config: ${ex.message}")
        }
    }

    // Helper function to make API requests with pagination for a specific date (config) with retry logic
    private fun fetchRows(offset: Int, length: Int, config: String): String {
        return try {
            bbcNewsWebClient()
                .get()
                .uri {
                    it.path("/rows")
                        .queryParam("dataset", "RealTimeData/bbc_news_alltime")
                        .queryParam("config", config) // Date (month) is passed as config
                        .queryParam("split", "train")
                        .queryParam("offset", offset.toString())
                        .queryParam("length", length.toString())
                        .build()
                }
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String::class.java)
                .retryWhen(
                    Retry.fixedDelay(3, Duration.ofSeconds(5)) // Retry up to 3 times with a 5-second delay
                        .filter { throwable ->
                            throwable is org.springframework.web.reactive.function.client.WebClientResponseException &&
                                    throwable.statusCode.is5xxServerError
                        } // Retry only on 5xx server errors
                )
                .block() ?: throw RuntimeException("Failed to fetch rows")
        } catch (ex: Exception) {
            println("Error fetching rows for config: $config, offset: $offset. Error: ${ex.message}")
            throw ex // Rethrow to propagate the error to fetchAndSaveNewsArticlesForDate
        }
    }

    // Helper function to parse the response JSON
    private fun parseResponse(response: String): JsonNode {
        val objectMapper = ObjectMapper()
        return objectMapper.readTree(response)
    }

    // Helper function to process each row and save it to MongoDB
    private fun processRows(rows: JsonNode) {
        rows.forEach { item ->
            val row = item.path("row")

            // Create the BbcRawNewsArticle object from the parsed row
            val newsArticle = BbcRawNewsArticle(
                title = row.path("title").asText(),
                publishedDate = LocalDate.parse(row.path("published_date").asText()),
                authors = row.path("authors").asText(),
                description = row.path("description").asText(),
                section = row.path("section").asText(),
                content = row.path("content").asText(),
                link = row.path("link").asText(),
                topImage = row.path("top_image").asText()
            )

            // Save the article to MongoDB
            bbcRawNewsRepository.save(newsArticle)
        }
    }
}
