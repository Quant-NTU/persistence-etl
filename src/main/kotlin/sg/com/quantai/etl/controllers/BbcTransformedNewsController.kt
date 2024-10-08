package sg.com.quantai.etl.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.data.BbcTransformedNewsArticle
import sg.com.quantai.etl.repositories.BbcTransformedNewsRepository

@RestController
@RequestMapping("/api/transformed-bbc-news")
class BbcTransformedNewsController(
    @Autowired private val bbcTransformedNewsRepository: BbcTransformedNewsRepository
) {
    // GET endpoint to retrieve all transformed BBC news articles
    @GetMapping("/all")
    fun getAllTransformedNews(): ResponseEntity<List<BbcTransformedNewsArticle>> {
        val articles = bbcTransformedNewsRepository.findAll()
        return if (articles.isNotEmpty()) {
            ResponseEntity.ok(articles)
        } else {
            ResponseEntity.noContent().build()
        }
    }
}
