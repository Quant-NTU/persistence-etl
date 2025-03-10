// CryptoController.kt
package sg.com.quantai.etl.controllers

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.cryptos.CryptoService
import sg.com.quantai.etl.services.cryptos.CryptoTransformationService

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

    @PostMapping("/transform")
    fun triggerTransformation(): ResponseEntity<String> {
        cryptoTransformationService.transformData()
        return ResponseEntity.ok("Transformation triggered!")
    }

    @GetMapping("/top-symbols")
    fun getTopCryptoSymbols(): ResponseEntity<List<String>> {
        val topSymbols = cryptoService.getTopCryptoSymbols()
        return ResponseEntity.ok(topSymbols)
    }

    @PostMapping("/historical/store-top")
    fun storeHistoricalDataForTopSymbols(): ResponseEntity<String> {
        return try {
            cryptoService.storeHistoricalDataForTopSymbols()
            ResponseEntity.ok("Historical data for top symbols stored successfully!")
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error storing historical data for top symbols: ${e.message}")
        }
    }
}