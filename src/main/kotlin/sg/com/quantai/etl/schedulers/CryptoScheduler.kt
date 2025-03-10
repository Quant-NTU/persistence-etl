package sg.com.quantai.etl.schedulers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import sg.com.quantai.etl.services.cryptos.CryptoService

@Component
class CryptoScheduler(private val cryptoService: CryptoService) {

    private val logger: Logger = LoggerFactory.getLogger(CryptoScheduler::class.java)

    @Scheduled(cron = "0 5 0 * * ?") // 12:05 AM UTC daily
    fun pullTopSymbols() {
        logger.info("Scheduled crypto pull for top symbols started...")
        try {
            cryptoService.storeHistoricalDataForTopSymbols()
            logger.info("Scheduled crypto pull for top symbols completed.")
        } catch (e: Exception) {
            logger.error("Error during scheduled crypto pull for top symbols: ${e.message}")
        }
    }
}