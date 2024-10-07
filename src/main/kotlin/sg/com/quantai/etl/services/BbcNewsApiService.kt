package sg.com.quantai.etl.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
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

@Service
class BbcNewsApiService(
    @Autowired private val bbcRawNewsRepository: BbcRawNewsRepository
) {

    private val newApiUrl = "https://datasets-server.huggingface.co/"
    private val batchSize = 100

    private fun bbcNewsWebClient(): WebClient {
        return WebClient.builder().baseUrl(newApiUrl)
            .codecs { it.defaultCodecs().maxInMemorySize(16 * 1024 * 1024) }
            .build()
    }

    fun fetchAndSaveNewsArticlesForAllMonths() {
        println("Loading of BBC News Articles - START")

        val startDate = YearMonth.of(2017, 1)
        val endDate = YearMonth.of(2024, 9)

        var currentMonth = startDate
        while (currentMonth <= endDate) {
            val configDate = currentMonth.toString()
            println("Fetching data for: $configDate")
            fetchAndSaveNewsArticlesForDate(configDate)
            currentMonth = currentMonth.plusMonths(1)
        }
        println("Loading of BBC News Articles - END")
    }

    private fun fetchAndSaveNewsArticlesForDate(config: String) {
        try {
            val initialResponse = fetchRows(0, batchSize, config)
            val rootNode: JsonNode = parseResponse(initialResponse)
            val totalRows = rootNode.path("num_rows_total").asInt()

            processRows(rootNode.path("rows"))

            for (offset in batchSize until totalRows step batchSize) {
                val response = fetchRows(offset, batchSize, config)
                val nextRootNode = parseResponse(response)
                processRows(nextRootNode.path("rows"))
            }

            println("Successfully saved all articles for $config.")
        } catch (ex: Exception) {
            println("Error fetching data for $config: ${ex.message}")
        }
    }

    private fun fetchRows(offset: Int, length: Int, config: String): String {
        return try {
            bbcNewsWebClient()
                .get()
                .uri {
                    it.path("/rows")
                        .queryParam("dataset", "RealTimeData/bbc_news_alltime")
                        .queryParam("config", config)
                        .queryParam("split", "train")
                        .queryParam("offset", offset.toString())
                        .queryParam("length", length.toString())
                        .build()
                }
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String::class.java)
                .retryWhen(
                    Retry.fixedDelay(3, Duration.ofSeconds(5))
                        .filter { throwable ->
                            throwable is org.springframework.web.reactive.function.client.WebClientResponseException &&
                                    throwable.statusCode.is5xxServerError
                        }
                )
                .block() ?: throw RuntimeException("Failed to fetch rows")
        } catch (ex: Exception) {
            println("Error fetching rows for config: $config, offset: $offset. Error: ${ex.message}")
            throw ex
        }
    }

    private fun parseResponse(response: String): JsonNode {
        val objectMapper = ObjectMapper()
        return objectMapper.readTree(response)
    }

    private fun processRows(rows: JsonNode) {
        rows.forEach { item ->
            val row = item.path("row")
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

            bbcRawNewsRepository.save(newsArticle)
        }
    }
}
