package sg.com.quantai.etl.schedulers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import sg.com.quantai.etl.services.stock.StockService

@Component
class StockScheduler(
    private val stockService: StockService
) {

    private val logger: Logger = LoggerFactory.getLogger(StockScheduler::class.java)

    @Scheduled(cron = "0 0 1 * * ?")
    fun pull() {
        logger.info("Scheduled stock pull started...")
        try {
            stockService.fetchAndSaveStockFromOneDrive()
            logger.info("Scheduled stock pull completed.")
        } catch (e: Exception) {
            logger.error("Error during scheduled pull for stock: ${e.message}")
        }
    }
}