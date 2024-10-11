package sg.com.quantai.etl.controllers

import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.exceptions.NewsArticleException
import sg.com.quantai.etl.repositories.NewsArticleRepository
import sg.com.quantai.etl.services.NewsArticleBBCService
import sg.com.quantai.etl.services.NewsArticleService

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RequestMapping

@RestController
@RequestMapping("/news_articles")
class NewsArticleController(
    @Autowired private val newsArticleService: NewsArticleService,
    @Autowired private val newsArticleBBCService: NewsArticleBBCService,
    @Autowired private val newsArticleRepository: NewsArticleRepository,
) {

    @PostMapping("/bbc/fetch")
    fun fetchAndSaveBBCNews(): ResponseEntity<String> {
        newsArticleBBCService.fetchAndSaveAllMonths()
        return ResponseEntity("News articles fetched and saved.", HttpStatus.OK)
    }

    @PostMapping("/bbc/transform")
    fun transformBBC(): ResponseEntity<String> {
        return try {
            newsArticleService.transformAndSave()
            ResponseEntity("Transformation completed successfully and results have been stored in database.", HttpStatus.OK)
        } catch (e: NewsArticleException) {
            ResponseEntity("Transformation failed: ${e.message}", HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    @GetMapping("/api/all")
    fun findAll(): ResponseEntity<List<NewsArticle>> {
        val articles = newsArticleRepository.findAll()
        return if (articles.isNotEmpty()) {
            ResponseEntity(articles, HttpStatus.OK)
        } else {
            ResponseEntity.noContent().build()
        }
    }


}
