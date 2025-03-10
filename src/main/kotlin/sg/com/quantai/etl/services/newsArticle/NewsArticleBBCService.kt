package sg.com.quantai.etl.services.newsArticle

import sg.com.quantai.etl.data.NewsArticleBBC
import sg.com.quantai.etl.exceptions.NewsArticleException
import sg.com.quantai.etl.repositories.NewsArticleBBCRepository

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Duration
import java.time.LocalDate
import java.time.YearMonth
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.util.retry.Retry

@Service
class NewsArticleBBCService(
    @Autowired private val newsArticlesBBCRepository: NewsArticleBBCRepository
) {

    @Value("\${quantai.external.api.newsarticle.bbc}")
    private lateinit var url: String
    private val batchSize = 100
    private val logger: Logger = LoggerFactory.getLogger(NewsArticleBBCService::class.java)

    private fun bbcNewsWebClient(): WebClient {
        return WebClient.builder().baseUrl(url)
            .codecs { it.defaultCodecs().maxInMemorySize(16 * 1024 * 1024) }
            .build()
    }

    fun fetchAndSaveAllMonths() {
        logger.info("Loading all BBC News Articles from HuggingFace API - START")

        val startDate = YearMonth.of(2017, 1)
        val endDate = YearMonth.of(2024, 9)

        var currentMonth = startDate
        while (currentMonth <= endDate) {
            fetchAndSaveForDate(currentMonth)
            currentMonth = currentMonth.plusMonths(1)
        }
        logger.info("Loading all BBC News Articles from HuggingFace API - END")
    }

    private fun fetchAndSaveForDate(month: java.time.YearMonth) {
        logger.info("Fetching BBC News Articles from month $month - START")

        val initialResponse = fetchRows(0, batchSize, month)
        val rootNode: JsonNode = parseResponse(initialResponse)
        val totalRows = rootNode.path("num_rows_total").asInt()

        saveNewsArticleBBC(rootNode.path("rows"))

        try {
            for (offset in batchSize until totalRows step batchSize) {
                val response = fetchRows(offset, batchSize, month)
                val nextRootNode = parseResponse(response)
                saveNewsArticleBBC(nextRootNode.path("rows"))
            }
        } catch (e: NewsArticleException) {
            logger.error("Error fetching BBC News Articles from month $month: ${e.message}")
        }

        logger.info("Fetching BBC News Articles from month $month - END")
    }

    private fun fetchRows(offset: Int, length: Int, month: java.time.YearMonth): String {
        return try {
            bbcNewsWebClient()
                .get()
                .uri {
                    it.path("/rows")
                        .queryParam("dataset", "RealTimeData/bbc_news_alltime")
                        .queryParam("config", month.toString())
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
                .block() ?: throw NewsArticleException("Failed to fetch rows for configDate: $month, offset: $offset, length: $length")
        } catch (e: Exception) {
            throw NewsArticleException("Failed with general error: ${e.message}")
        }
    }

    private fun parseResponse(response: String): JsonNode {
        val objectMapper = ObjectMapper()
        return objectMapper.readTree(response)
    }

    private fun saveNewsArticleBBC(json: JsonNode) {
        json.forEach {
            val row = it.path("row")
            val link = row.path("link").asText()

            if (!newsArticlesBBCRepository.existsByLink(link)) {
                val newsArticle = NewsArticleBBC(
                    title = row.path("title").asText(),
                    publishedDate = LocalDate.parse(row.path("published_date").asText()),
                    description = row.path("description").asText(),
                    content = row.path("content").asText(),
                    link = row.path("link").asText(),
                    topImage = row.path("top_image").asText(),
                    transformed = false
                )

                if (!row.path("authors").isNull)
                    newsArticle.authors = row.path("authors").asText()
                if (!row.path("section").isNull)
                    newsArticle.section = row.path("section").asText()

                newsArticlesBBCRepository.save(newsArticle)
            } else {
                logger.info("Article with link: $link already exists, skipping insertion.")
            }
        }
    }
}
