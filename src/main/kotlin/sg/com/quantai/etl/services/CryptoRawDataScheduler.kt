package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class CryptoRawDataScheduler(private val cryptoService: CryptoService) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoRawDataScheduler::class.java)

    /**
     * Schedule the raw data fetching task for top symbols to run daily at 12:05 AM UTC.
     */
    @Scheduled(cron = "0 5 0 * * ?") // 12:05 AM UTC daily
    fun scheduleTopSymbolsDataFetch() {
        logger.info("Scheduled data fetch for top symbols started...")

        try {
            cryptoService.storeHistoricalDataForTopSymbols()
            logger.info("Scheduled data fetch for top symbols completed.")
        } catch (e: Exception) {
            logger.error("Error during scheduled data fetch for top symbols: ${e.message}")
        }
    }
}