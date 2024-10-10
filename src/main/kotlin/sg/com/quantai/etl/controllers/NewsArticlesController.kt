package sg.com.quantai.etl.controllers

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.repositories.NewsArticlesRepository
import sg.com.quantai.etl.services.NewsArticlesBBCService
import sg.com.quantai.etl.services.NewsArticlesService

@RestController
@RequestMapping("/news_articles")
class NewsArticlesController(
    @Autowired private val newsArticlesService: NewsArticlesService,
    @Autowired private val newsArticlesBBCService: NewsArticlesBBCService,
    @Autowired private val newsArticlesRepository: NewsArticlesRepository,
) {

    @PostMapping("/bbc/fetch")
    fun fetchAndSaveNews(): ResponseEntity<String> {
        newsArticlesBBCService.fetchAndSaveNewsArticlesForAllMonths()
        return ResponseEntity("News articles fetched and saved.", HttpStatus.OK)
    }

    @PostMapping("/bbc/transform")
    fun runTransformation(): ResponseEntity<String> {
        try {
            newsArticlesService.transformAndSaveNewsArticles()
            return ResponseEntity("Transformation completed successfully and results have been stored in database.", HttpStatus.OK)
        } catch (ex: Exception) {
            return ResponseEntity("Transformation failed: ${ex.message}", HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    @GetMapping("/api/all")
    fun getAllTransformedNews(): ResponseEntity<List<NewsArticle>> {
        val articles = newsArticlesRepository.findAll()
        return if (articles.isNotEmpty()) {
            ResponseEntity.ok(articles)
        } else {
            ResponseEntity.noContent().build()
        }
    }


}
