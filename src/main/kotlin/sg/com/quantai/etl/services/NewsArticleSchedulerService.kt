package sg.com.quantai.etl.services

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired

@Service
class NewsArticleSchedulerService(
    @Autowired private val newsArticleBBCService: NewsArticleBBCService,
    @Autowired private val newsArticleService: NewsArticleService
) {

    private val logger: Logger = LoggerFactory.getLogger(NewsArticleSchedulerService::class.java)

    @Scheduled(cron = "0 0 2 15 * ?")
    fun scheduleFetchAndTransformNews() {
        logger.info("Scheduled News Fetch - START")

        newsArticleBBCService.fetchAndSaveAllMonths()

        logger.info("Scheduled News Fetch - END")

        logger.info("Scheduled News Transformation - START")

        newsArticleService.transformAndSave()

        logger.info("Scheduled News Transformation - END")
    }
}