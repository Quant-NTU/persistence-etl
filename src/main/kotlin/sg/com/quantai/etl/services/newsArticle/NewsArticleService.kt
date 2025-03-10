package sg.com.quantai.etl.services.newsArticle

import sg.com.quantai.etl.data.NewsArticleBBC
import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.repositories.NewsArticleBBCRepository
import sg.com.quantai.etl.repositories.NewsArticleRepository

import java.util.stream.Collectors
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class NewsArticleService(
    @Autowired private val newsArticlesBBCRepository: NewsArticleBBCRepository,
    @Autowired private val newsArticleRepository: NewsArticleRepository
) {
    private val logger: Logger = LoggerFactory.getLogger(NewsArticleService::class.java)

    fun transformAndSave() {
        logger.info("Transforming all BBC News Articles - START")
        val untransformedArticles: List<NewsArticleBBC> = newsArticlesBBCRepository.findAllByTransformedFalse()

        val transformedArticles: List<NewsArticle> = untransformedArticles.stream()
            .filter {
                it.title.isNotBlank() && it.description?.isNotBlank() == true && it.content.isNotBlank()
            }
            .map {
                NewsArticle(
                    title = it.title,
                    publishedDate = it.publishedDate,
                    description = it.description ?: "",
                    content = it.content
                )
            }.collect(Collectors.toList())

        newsArticleRepository.saveAll(transformedArticles)

        untransformedArticles.forEach {
            it.transformed = true
            newsArticlesBBCRepository.save(it)
        }

        logger.info("Transformation completed. ${transformedArticles.size} transformed articles loaded into database.")
        logger.info("Transforming all BBC News Articles - END")
    }

}