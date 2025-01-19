package sg.com.quantai.etl.controllers

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.CryptoService

@RestController
@RequestMapping("/crypto")
class CryptoController(private val cryptoService: CryptoService) {

    /**
     * Fetch the current price of a cryptocurrency
     * Example Usage:
     * GET /crypto/current/price?symbol=BTC&currency=USD
     */
    @GetMapping("/price/current")
    fun getCryptoPrice(
        @RequestParam symbol: String,
        @RequestParam currency: String
    ): ResponseEntity<JsonNode> {
        val response = cryptoService.fetchCryptoPrice(symbol, currency)
        return if (response != null) {
            ResponseEntity.ok(response)
        } else {
            ResponseEntity.status(500).body(null)
        }
    }

    /**
     * Fetch and store historical cryptocurrency data
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
}