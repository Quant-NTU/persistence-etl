package sg.com.quantai.etl.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import sg.com.quantai.etl.data.NewsArticleBBC
import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.repositories.NewsArticlesBBCRepository
import sg.com.quantai.etl.repositories.NewsArticlesRepository
import java.util.stream.Collectors

@Service
class NewsArticlesService(
    @Autowired private val newsArticlesBBCRepository: NewsArticlesBBCRepository,
    @Autowired private val newsArticlesRepository: NewsArticlesRepository
) {
    fun transformAndSaveNewsArticles() {
        println("Transformation started.")
        val rawArticles: List<NewsArticleBBC> = newsArticlesBBCRepository.findAll()

        val transformedArticles: List<NewsArticle> = rawArticles.stream()
            .filter {
                it.title.isNotBlank() && it.description?.isNotBlank() == true && it.content.isNotBlank()
            }
            .map { rawArticle ->
                NewsArticle(
                    title = rawArticle.title,
                    publishedDate = rawArticle.publishedDate,
                    description = rawArticle.description ?: "",
                    content = rawArticle.content
                )
            }.collect(Collectors.toList())

        newsArticlesRepository.saveAll(transformedArticles)

        println("Transformation completed. ${transformedArticles.size} transformed articles loaded into database.")
    }

}