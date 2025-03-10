package sg.com.quantai.etl.schedulers

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import sg.com.quantai.etl.services.newsArticle.NewsArticleBBCService

@Component
class NewsArticleScheduler(
    @Autowired private val newsArticleBBCService: NewsArticleBBCService
) {

    private val logger: Logger = LoggerFactory.getLogger(NewsArticleScheduler::class.java)

    @Scheduled(cron = "0 0 2 15 * ?")
    fun pull() {
        logger.info("Scheduled news article pull task started...")
        try {
            newsArticleBBCService.fetchAndSaveAllMonths()
            logger.info("Scheduled news article pull task completed.")
        } catch (e: Exception) {
            logger.error("Error during pull task: ${e.message}")
        }
    }
}