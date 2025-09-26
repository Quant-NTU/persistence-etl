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
        @RequestParam(defaultValue = "30") outputsize: Int
    ): ResponseEntity<String> {
        return try {
            stockService.fetchAndStoreHistoricalData(symbol, outputsize)
            ResponseEntity.ok("Historical data for $symbol fetched and stored successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error fetching data for $symbol: ${e.message}")
        }
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
