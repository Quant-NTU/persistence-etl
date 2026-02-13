package sg.com.quantai.etl.controllers

import sg.com.quantai.etl.services.ForexService
import sg.com.quantai.etl.services.ForexTransformationService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/forex")
class ForexController(
    private val forexService: ForexService,
    private val forexTransformationService: ForexTransformationService
) {

    @GetMapping("/top-pairs")
    fun getTopForexPairs(): ResponseEntity<List<String>> {
        val pairs = forexService.getTopForexPairs()
        return ResponseEntity.ok(pairs)
    }

    @PostMapping("/historical/store")
    fun fetchAndStoreHistoricalData(
        @RequestParam currencyPair: String,
        @RequestParam(defaultValue = "30") limit: Int,
        @RequestParam(defaultValue = "1day") interval: String
    ): ResponseEntity<String> {
        return try {
            val supportedIntervals = forexService.getSupportedIntervals()
            if (!supportedIntervals.contains(interval)) {
                return ResponseEntity.badRequest().body("Unsupported interval: $interval. Supported intervals: ${supportedIntervals.joinToString(", ")}")
            }
            forexService.fetchAndStoreHistoricalData(currencyPair, limit, interval)
            ResponseEntity.ok("Historical data for $currencyPair (interval: $interval) fetched and stored successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error fetching data for $currencyPair: ${e.message}")
        }
    }

    @GetMapping("/intervals")
    fun getSupportedIntervals(): ResponseEntity<List<String>> {
        val intervals = forexService.getSupportedIntervals()
        return ResponseEntity.ok(intervals)
    }

    @PostMapping("/historical/store-top")
    fun fetchAndStoreHistoricalDataForTopPairs(): ResponseEntity<String> {
        return try {
            forexService.storeHistoricalDataForTopPairs()
            ResponseEntity.ok("Historical data for top forex pairs fetched and stored successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error fetching top forex data: ${e.message}")
        }
    }

    @PostMapping("/transform")
    fun triggerDataTransformation(): ResponseEntity<String> {
        return try {
            forexTransformationService.transformData()
            ResponseEntity.ok("Forex data transformation completed successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error during forex data transformation: ${e.message}")
        }
    }

    /**
     * Fetch historical price data for a forex pair (for chart display)
     * Example Usage:
     * GET /forex/price-history?currencyPair=EUR/USD&days=30
     */
    @GetMapping("/price-history")
    fun getPriceHistory(
        @RequestParam currencyPair: String,
        @RequestParam(defaultValue = "30") days: Int
    ): ResponseEntity<Map<String, Any>> {
        return try {
            val effectiveDays = days.coerceIn(1, 365)
            val data = forexService.fetchPriceHistory(currencyPair, effectiveDays)
            
            ResponseEntity.ok(mapOf(
                "status" to "success",
                "currencyPair" to currencyPair,
                "interval" to "1day",
                "days" to effectiveDays,
                "count" to data.size,
                "data" to data
            ))
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body(mapOf(
                "status" to "error",
                "message" to "Error fetching price history: ${e.message}"
            ))
        }
    }
}
