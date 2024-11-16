package sg.com.quantai.etl.controllers

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import sg.com.quantai.etl.services.CryptoService

@RestController
class CryptoController(private val cryptoService: CryptoService) {

    // Endpoint to fetch current crypto price
    @GetMapping("/crypto/current/price")
    fun getCryptoPrice(
        @RequestParam symbol: String,
        @RequestParam currency: String
    ): JsonNode {
        return cryptoService.fetchCryptoPrice(symbol, currency)
    }

    // Endpoint to fetch historical crypto data
    @GetMapping("/crypto/historical/price")
    fun getHistoricalData(
        @RequestParam symbol: String,
        @RequestParam currency: String,
        @RequestParam limit: Int
    ): JsonNode {
        return cryptoService.fetchHistoricalData(symbol, currency, limit)
    }

    // Endpoint to fetch and store historical crypto data into the database
    @GetMapping("/crypto/historical/store")
    fun fetchAndStoreHistoricalData(
        @RequestParam symbol: String,
        @RequestParam currency: String,
        @RequestParam limit: Int
    ): String {
        cryptoService.fetchAndStoreHistoricalData(symbol, currency, limit)
        return "Historical data for $symbol-$currency successfully stored!"
    }
}