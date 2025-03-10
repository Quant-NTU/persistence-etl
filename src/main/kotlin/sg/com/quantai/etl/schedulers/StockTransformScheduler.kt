package sg.com.quantai.etl.schedulers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import sg.com.quantai.etl.services.stock.StockService
import sg.com.quantai.etl.services.stock.StockTransformService

@Component
class StockTransformScheduler(
    private val stockService: StockService,
    private val stockTransformService: StockTransformService
) {
    private val logger: Logger = LoggerFactory.getLogger(StockScheduler::class.java)

    @Scheduled(cron = "0 0 2 * * ?")
    fun transform() {
        logger.info("Scheduled stock transform started...")
        try {
            stockTransformService.transform()
            logger.info("Scheduled stock transform completed.")
        } catch (e: Exception) {
            logger.error("Error during scheduled transform for stock: ${e.message}")
        }
    }
}