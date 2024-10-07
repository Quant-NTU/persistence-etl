package sg.com.quantai.etl.controllers
import sg.com.quantai.etl.repositories.BbcRawNewsRepository
import sg.com.quantai.etl.data.BbcRawNewsArticle
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.requests.BbcRawNewsArticleRequest
import sg.com.quantai.etl.services.BbcNewsApiService
import java.time.LocalDate

@RestController
@RequestMapping("/news")
class BbcRawNewsController(
    private val bbcRawNewsRepository: BbcRawNewsRepository,
    private val bbcNewsApiService: BbcNewsApiService
) {
    @GetMapping
    fun getAllNewsArticles(): ResponseEntity<List<BbcRawNewsArticle>> {
        val articles = bbcRawNewsRepository.findAll()
        return ResponseEntity.ok(articles)
    }

    @GetMapping("/{uuid}")
    fun getOneNewsArticle(@PathVariable("uuid") uuid: String): ResponseEntity<BbcRawNewsArticle> {
        val article = bbcRawNewsRepository.findOneByUuid(uuid)
        return ResponseEntity.ok(article)
    }

    @PostMapping("/add")
    fun addNewsArticle(
        @RequestBody article: BbcRawNewsArticleRequest
    ): ResponseEntity<BbcRawNewsArticle> {
        val newsArticle = bbcRawNewsRepository.save(
            BbcRawNewsArticle(
                title = article.title,
                publishedDate = LocalDate.parse(article.publishedDate),
                authors = article.authors,
                description = article.description,
                section = article.section,
                content = article.content,
                link = article.link,
                topImage = article.topImage
            )
        )
        return ResponseEntity(newsArticle, HttpStatus.CREATED)
    }

    @PostMapping("/fetch")
    fun fetchAndSaveNews(): ResponseEntity<String> {
        bbcNewsApiService.fetchAndSaveNewsArticlesForAllMonths()
        return ResponseEntity("News articles fetched and saved.", HttpStatus.OK)
    }
}
