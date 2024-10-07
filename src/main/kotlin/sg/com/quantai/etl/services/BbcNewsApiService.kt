package sg.com.quantai.etl.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import sg.com.quantai.etl.data.BbcRawNewsArticle
import sg.com.quantai.etl.repositories.BbcRawNewsRepository
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.LocalDateTime
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono


//  BBCNewsApiService handles API requests and data insertion into MongoDB.
@Service
class BbcNewsApiService(
    @Autowired private val bbcRawNewsRepository: BbcRawNewsRepository,
    @Autowired private val restTemplate: RestTemplate // To make HTTP calls
) {

    private val apiUrl = "https://datasets-server.huggingface.co/rows?dataset=RealTimeData%2Fbbc_news_alltime&config=2017-01&split=train&offset=0&length=100"
    private val webClient = WebClient.builder().build()

    private val newApiUrl = "https://datasets-server.huggingface.co/rows"

    private fun bbcNewsWebClient() : WebClient {
        return WebClient.builder().baseUrl(newApiUrl).build()
    }

    fun fetchAndSaveNewsArticles() {
        bbcNewsWebClient()
            .get()
            .uri("?dataset=RealTimeData%2Fbbc_news_alltime&config=2017-01&split=train&offset=0&length=100")
            .retrieve()
            .toEntity(String::class.java)
            .block()


//        // Make the API request using WebClient
//        webClient.get()
//            .uri(apiUrl)
//            .retrieve()
//            .bodyToMono(String::class.java) // Get the response body as a Mono of String
//            .doOnNext { response ->
//                // Process the response using ObjectMapper
//                val objectMapper = ObjectMapper()
//                val rootNode: JsonNode = objectMapper.readTree(response)
//                val rows = rootNode.path("rows")
//
//                rows.forEach { item ->
//                    val row = item.path("row")
//                    val newsArticle = BbcRawNewsArticle(
//                        title = row.path("title").asText(),
//                        publishedDate = LocalDateTime.parse(row.path("published_date").asText()),
//                        authors = row.path("authors").asText(),
//                        description = row.path("description").asText(),
//                        section = row.path("section").asText(),
//                        content = row.path("content").asText(),
//                        link = row.path("link").asText(),
//                        topImage = row.path("top_image").asText()
//                    )
//                    // Save the article to MongoDB
//                    bbcRawNewsRepository.save(newsArticle)
//                }
//            }
//            .onErrorResume { ex ->
//                // Handle errors (like 500 Internal Server Errors)
//                println("Error occurred while fetching news articles: ${ex.message}")
//                Mono.empty() // Return an empty Mono to continue
//            }
//            .block() // Block the execution to make it synchronous
    }
}