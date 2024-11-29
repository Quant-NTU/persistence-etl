package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class CryptoTransformationScheduler(private val transformationService: CryptoTransformationService) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoTransformationScheduler::class.java)

    /**
     * Schedule the transformation task to run daily at midnight.
     */
    @Scheduled(cron = "0 0 0 * * ?")
    fun scheduleTransformation() {
        logger.info("Scheduled transformation task started...")
        transformationService.transformData()
        logger.info("Scheduled transformation task completed.")
    }
}
