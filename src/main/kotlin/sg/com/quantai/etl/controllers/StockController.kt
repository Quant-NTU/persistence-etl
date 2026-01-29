package sg.com.quantai.etl.controllers

import sg.com.quantai.etl.services.StockService
import sg.com.quantai.etl.services.StockTransformationService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/stock")
class StockController(
    private val stockService: StockService,
    private val stockTransformationService: StockTransformationService
) {

    @GetMapping("/top-symbols")
    fun getTopStockSymbols(): ResponseEntity<List<String>> {
        val symbols = stockService.getTopStockSymbols()
        return ResponseEntity.ok(symbols)
    }

    @PostMapping("/historical/store")
    fun fetchAndStoreHistoricalData(
        @RequestParam symbol: String,
        @RequestParam(defaultValue = "30") limit: Int,
        @RequestParam(defaultValue = "1day") interval: String
    ): ResponseEntity<String> {
        return try {
            val supportedIntervals = stockService.getSupportedIntervals()
            if (!supportedIntervals.contains(interval)) {
                return ResponseEntity.badRequest().body("Unsupported interval: $interval. Supported intervals: ${supportedIntervals.joinToString(", ")}")
            }
            stockService.fetchAndStoreHistoricalData(symbol, limit, interval)
            ResponseEntity.ok("Historical data for $symbol (interval: $interval) fetched and stored successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error fetching data for $symbol: ${e.message}")
        }
    }

    @GetMapping("/intervals")
    fun getSupportedIntervals(): ResponseEntity<List<String>> {
        val intervals = stockService.getSupportedIntervals()
        return ResponseEntity.ok(intervals)
    }

    @PostMapping("/historical/store-top")
    fun fetchAndStoreHistoricalDataForTopSymbols(): ResponseEntity<String> {
        return try {
            stockService.storeHistoricalDataForTopSymbols()
            ResponseEntity.ok("Historical data for top stock symbols fetched and stored successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error fetching top stock data: ${e.message}")
        }
    }

    @PostMapping("/historical/store-by-date")
    fun fetchAndStoreHistoricalDataByDate(
        @RequestParam symbol: String,
        @RequestParam startDate: String,
        @RequestParam endDate: String,
        @RequestParam(defaultValue = "1day") interval: String
    ): ResponseEntity<String> {
        return try {
            val supportedIntervals = stockService.getSupportedIntervals()
            if (!supportedIntervals.contains(interval)) {
                return ResponseEntity.badRequest()
                    .body("Unsupported interval: $interval. Supported intervals: ${supportedIntervals.joinToString(", ")}")
            }

            stockService.fetchAndStoreHistoricalDataByDate(
                symbol = symbol,
                interval = interval,
                startDate = startDate,
                endDate = endDate
            )

            ResponseEntity.ok(
                "Historical data for $symbol (interval=$interval) from $startDate to $endDate successfully stored!"
            )
        } catch (e: Exception) {
            ResponseEntity.internalServerError()
                .body("Error storing historical data for $symbol: ${e.message}")
        }
    }


    @PostMapping("/transform")
    fun triggerDataTransformation(): ResponseEntity<String> {
        return try {
            stockTransformationService.transformData()
            ResponseEntity.ok("Stock data transformation completed successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error during stock data transformation: ${e.message}")
        }
    }
}
