package sg.com.quantai.etl.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import sg.com.quantai.etl.data.BbcRawNewsArticle
import sg.com.quantai.etl.repositories.BbcRawNewsRepository
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.LocalDateTime

//  BBCNewsApiService handles API requests and data insertion into MongoDB.
@Service
class BbcNewsApiService(
    @Autowired private val bbcRawNewsRepository: BbcRawNewsRepository,
    @Autowired private val restTemplate: RestTemplate // To make HTTP calls
) {

    private val apiUrl = "https://datasets-server.huggingface.co/rows?dataset=RealTimeData%2Fbbc_news_alltime&config=2017-01&split=train&offset=0&length=100"

    fun fetchAndSaveNewsArticles() {
        // Make the API request
        val response = restTemplate.getForObject(apiUrl, String::class.java)

        response?.let {
            // Parse the JSON response
            val objectMapper = ObjectMapper()
            val rootNode: JsonNode = objectMapper.readTree(it)
            val rows = rootNode.path("rows")

            rows.forEach { item ->
                val row = item.path("row")
                val newsArticle = BbcRawNewsArticle(
                    title = row.path("title").asText(),
                    publishedDate = LocalDateTime.parse(row.path("published_date").asText()),
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
}