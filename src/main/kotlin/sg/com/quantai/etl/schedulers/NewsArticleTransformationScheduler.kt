package sg.com.quantai.etl.schedulers

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import sg.com.quantai.etl.services.newsArticle.NewsArticleBBCService
import sg.com.quantai.etl.services.newsArticle.NewsArticleService

@Component
class NewsArticleTransformationScheduler(
    @Autowired private val newsArticleService: NewsArticleService
) {

    private val logger: Logger = LoggerFactory.getLogger(NewsArticleTransformationScheduler::class.java)

    @Scheduled(cron = "0 0 2 15 * ?")
    fun transform() {
        logger.info("Scheduled news article transformation task started...")
        try {
            newsArticleService.transformAndSave()
            logger.info("Scheduled news article transformation task completed.")
        } catch (e: Exception) {
            logger.error("Error during fetch transformation: ${e.message}")
        }
    }
}