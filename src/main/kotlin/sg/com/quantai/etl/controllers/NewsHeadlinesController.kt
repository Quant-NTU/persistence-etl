package sg.com.quantai.etl.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.data.BbcTransformedNewsArticle
import sg.com.quantai.etl.repositories.BbcTransformedNewsRepository
import sg.com.quantai.etl.services.BbcNewsApiService
import sg.com.quantai.etl.services.BbcNewsTransformationService

@RestController
@RequestMapping("/news_headlines")
class NewsHeadlinesController(
    @Autowired private val bbcNewsTransformationService: BbcNewsTransformationService,
    @Autowired private val bbcNewsApiService: BbcNewsApiService,
    @Autowired private val bbcTransformedNewsRepository: BbcTransformedNewsRepository,
) {

    @PostMapping("/bbc/fetch")
    fun fetchAndSaveNews(): ResponseEntity<String> {
        bbcNewsApiService.fetchAndSaveNewsArticlesForAllMonths()
        return ResponseEntity("News articles fetched and saved.", HttpStatus.OK)
    }

    @PostMapping("/bbc/transform")
    fun runTransformation(): ResponseEntity<String> {
        try {
            bbcNewsTransformationService.transformAndSaveNewsArticles()
            return ResponseEntity("Transformation completed successfully and results have been stored in database.", HttpStatus.OK)
        } catch (ex: Exception) {
            return ResponseEntity("Transformation failed: ${ex.message}", HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    @GetMapping("/bbc/api/news_transformed")
    fun getAllTransformedNews(): ResponseEntity<List<BbcTransformedNewsArticle>> {
        val articles = bbcTransformedNewsRepository.findAll()
        return if (articles.isNotEmpty()) {
            ResponseEntity.ok(articles)
        } else {
            ResponseEntity.noContent().build()
        }
    }


}
