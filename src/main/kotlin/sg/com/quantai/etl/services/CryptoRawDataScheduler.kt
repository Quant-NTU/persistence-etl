package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class CryptoRawDataScheduler(private val cryptoService: CryptoService) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoRawDataScheduler::class.java)

    /**
     * Schedule the raw data fetching task to run daily at 1:00 AM.
     */
    @Scheduled(cron = "0 30 0 * * ?") // 12:30 AM UTC daily
    fun scheduleRawDataFetch() {
        logger.info("Scheduled raw data fetch started...")

        try {
            // Add the symbols and currencies to fetch
            val symbols = listOf("BTC", "ETH")
            val currencies = listOf("USD", "EUR")

            symbols.forEach { symbol ->
                currencies.forEach { currency ->
                    logger.info("Fetching raw data for $symbol in $currency")
                    cryptoService.fetchAndStoreHistoricalData(symbol, currency, 1)
                }
            }

            logger.info("Scheduled raw data fetch completed.")
        } catch (e: Exception) {
            logger.error("Error during scheduled raw data fetch: ${e.message}")
        }
    }
}
