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


//  BBCNewsApiService handles API requests and data insertion into MongoDB.
@Service
class BbcNewsApiService(
    @Autowired private val bbcRawNewsRepository: BbcRawNewsRepository,
    @Autowired private val restTemplate: RestTemplate // To make HTTP calls
) {

    private val newApiUrl = "https://datasets-server.huggingface.co/"
    private val batchSize = 100 // Number of rows to fetch per request

    // WebClient configuration
    private fun bbcNewsWebClient(): WebClient {
        return WebClient.builder().baseUrl(newApiUrl)
            .codecs { it.defaultCodecs().maxInMemorySize(16 * 1024 * 1024) } // Increase buffer size to 16MB
            .build()
    }

    // Fetch and save all articles from the API
    fun fetchAndSaveNewsArticles() {
        // Step 1: Fetch the first batch to get the total number of rows
        val initialResponse = fetchRows(0, batchSize)
        val rootNode: JsonNode = parseResponse(initialResponse)
        val totalRows = rootNode.path("num_rows_total").asInt()

        // Process the first batch of rows
        processRows(rootNode.path("rows"))

        // Step 2: Fetch remaining rows in batches of 100
        for (offset in batchSize until totalRows step batchSize) {
            val response = fetchRows(offset, batchSize)
            val nextRootNode = parseResponse(response)
            processRows(nextRootNode.path("rows"))
        }

        print("Successfully saved all articles.")
    }

    // Helper function to make API requests with pagination
    private fun fetchRows(offset: Int, length: Int): String {
        return bbcNewsWebClient()
            .get()
            .uri {
                it.path("/rows")
                    .queryParam("dataset", "RealTimeData/bbc_news_alltime")
                    .queryParam("config", "2017-01")
                    .queryParam("split", "train")
                    .queryParam("offset", offset.toString())
                    .queryParam("length", length.toString())
                    .build()
            }
            .accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .toEntity(String::class.java)
            .block()?.body ?: throw RuntimeException("Failed to fetch rows")
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