// CryptoController.kt
package sg.com.quantai.etl.controllers

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.CryptoService
import sg.com.quantai.etl.services.CryptoTransformationService

@RestController
@RequestMapping("/crypto")
class CryptoController(
    private val cryptoService: CryptoService,
    private val cryptoTransformationService: CryptoTransformationService
) {

    /**
     * Fetch and store historical cryptocurrency data (manual trigger)
     * Example Usage:
     * POST /crypto/historical/store?symbol=BTC&currency=USD&limit=10
     */
    @PostMapping("/historical/store")
    fun fetchAndStoreHistoricalData(
        @RequestParam symbol: String,
        @RequestParam currency: String,
        @RequestParam limit: Int
    ): ResponseEntity<String> {
        return try {
            cryptoService.fetchAndStoreHistoricalData(symbol, currency, limit)
            ResponseEntity.ok("Historical data for $symbol-$currency successfully stored!")
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error storing historical data: ${e.message}")
        }
    }

    /**
     * Fetch and store historical cryptocurrency data by date
     * Example Usage:
     * POST /crypto/historical/store?symbol=BTC&currency=USD&startDate=2025-10-01&endDate=2025-10-31
     */
    @PostMapping("/historical/store-by-date")
    fun fetchAndStoreHistoricalDataByDate(
        @RequestParam symbol: String,
        @RequestParam currency: String = "USD", //USD by default  
        @RequestParam startDate: String,
        @RequestParam endDate: String
    ): ResponseEntity<String> {
        return try {
            cryptoService.fetchAndStoreHistoricalDataByDate(symbol, currency, startDate, endDate)
            ResponseEntity.ok("Historical data for $symbol-$currency from $startDate to $endDate successfully stored!")
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error storing historical data: ${e.message}")
        }
    }

    /**
     * Trigger data transformation
     */
    @PostMapping("/transform")
    fun triggerTransformation(): ResponseEntity<String> {
        cryptoTransformationService.transformData()
        return ResponseEntity.ok("Transformation triggered!")
    }

    /**
     * Fetch the top 10 cryptocurrency symbols
     * Example Usage:
     * GET /crypto/top-symbols
     */
    @GetMapping("/top-symbols")
    fun getTopCryptoSymbols(): ResponseEntity<List<String>> {
        val topSymbols = cryptoService.getTopCryptoSymbols()
        return ResponseEntity.ok(topSymbols)
    }

    /**
     * Fetch and store historical data for the top 10 cryptocurrency symbols (daily automatic)
     * Example Usage:
     * POST /crypto/historical/store-top
     */
    @PostMapping("/historical/store-top")
    fun storeHistoricalDataForTopSymbols(): ResponseEntity<String> {
        return try {
            cryptoService.storeHistoricalDataForTopSymbols()
            ResponseEntity.ok("Historical data for top symbols stored successfully!")
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error storing historical data for top symbols: ${e.message}")
        }
    }

    /**
     * Fetch but not store historical data for the specified ticker
     * Example Usage:
     * GET /crypto/price-by-date
     */
    @GetMapping("/price-by-date")
    fun getPriceByDate(
        @RequestParam symbol: String,
        @RequestParam date: String,
        @RequestParam(defaultValue = "USD") currency: String
    ): ResponseEntity<Any> {
        val price = cryptoService.getClosePriceByDate(symbol, currency, date)
            ?: return ResponseEntity.status(404).body("No price found")

        return ResponseEntity.ok(mapOf(
            "symbol" to symbol,
            "currency" to currency,
            "date" to date,
            "close" to price
        ))
    }

    /**
     * Fetch historical price data for a cryptocurrency (for chart display)
     * Example Usage:
     * GET /crypto/price-history?symbol=BTC&currency=USD&days=30
     */
    @GetMapping("/price-history")
    fun getPriceHistory(
        @RequestParam symbol: String,
        @RequestParam(defaultValue = "USD") currency: String,
        @RequestParam(defaultValue = "30") days: Int
    ): ResponseEntity<Map<String, Any>> {
        return try {
            val effectiveDays = days.coerceIn(1, 365)
            val data = cryptoService.fetchPriceHistory(symbol, currency, effectiveDays)
            
            ResponseEntity.ok(mapOf(
                "status" to "success",
                "symbol" to symbol,
                "currency" to currency,
                "days" to effectiveDays,
                "count" to data.size,
                "data" to data
            ))
        } catch (e: Exception) {
            ResponseEntity.status(500).body(mapOf(
                "status" to "error",
                "message" to "Error fetching price history: ${e.message}"
            ))
        }
    }
}