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

    private val apiUrl = "https://datasets-server.huggingface.co/rows?dataset=RealTimeData%2Fbbc_news_alltime&config=2017-01&split=train&offset=0&length=100"
    private val webClient = WebClient.builder().build()

    private val newApiUrl = "https://datasets-server.huggingface.co/"

    private fun bbcNewsWebClient() : WebClient {
        return WebClient.builder().baseUrl(newApiUrl).codecs { it.defaultCodecs().maxInMemorySize(16 * 1024 * 1024) }.build() // Error Fix 2: But then, I got a size problem. WebClient has a default buffer of 256 KB (256 * 1014). So I just increase its size to 16 MB
    }

    fun fetchAndSaveNewsArticles() {
        val response = bbcNewsWebClient()
            .get()
            .uri {
                it.path("/rows")
                    .queryParam(
                        "dataset",
                        "RealTimeData/bbc_news_alltime"
                    ) // Error Fix 1: When I decided to refactor the code to use queryParam, I changed the ASCII code "%2F" to "/" and this solved the bug
                    .queryParam("config", "2017-01")
                    .queryParam("split", "train")
                    .queryParam("offset", "0")
                    .queryParam("length", "100")
                    .build()
            }
            .accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .toEntity(String::class.java)
            .block()

        print(response)
        response?.body?.let {
            // Parse the JSON response using ObjectMapper
            val objectMapper = ObjectMapper()
            val rootNode: JsonNode = objectMapper.readTree(it)
            val rows = rootNode.path("rows")

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
        print("Succesfully saved all articles.")
    }
}