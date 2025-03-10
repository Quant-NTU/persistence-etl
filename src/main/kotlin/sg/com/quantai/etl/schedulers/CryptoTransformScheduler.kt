package sg.com.quantai.etl.schedulers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import sg.com.quantai.etl.services.cryptos.CryptoTransformService

@Component
class CryptoTransformScheduler(private val cryptoTransformService: CryptoTransformService) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoTransformScheduler::class.java)

    @Scheduled(cron = "0 30 0 * * ?") // 12:30 AM UTC daily
    fun transform() {
        logger.info("Scheduled crypto transformation task started...")
        try {
            cryptoTransformService.transformData()
            logger.info("Scheduled crypto transformation task completed.")
        } catch (e: Exception) {
            logger.error("Error during transformation task: ${e.message}")
        }
    }
}
