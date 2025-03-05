package sg.com.quantai.etl.controllers

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.StockDataService
import sg.com.quantai.etl.services.StockDataTransformationService

@RestController
@RequestMapping("/stock-data")
class StockDataController(
    private val stockDataService: StockDataService,
    private val transformationService: StockDataTransformationService
) {

    /**
     * Manually trigger stock data import from OneDrive
     */
    @PostMapping("/import")
    fun importStockData(): ResponseEntity<String> {
        return try {
            stockDataService.importStockDataFromOneDrive()
            ResponseEntity.ok("Stock data import triggered successfully!")
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error triggering stock data import: ${e.message}")
        }
    }

    /**
     * Manually trigger stock data transformation
     */
    @PostMapping("/transform")
    fun transformStockData(): ResponseEntity<String> {
        return try {
            transformationService.manuallyTriggerTransformation()
            ResponseEntity.ok("Stock data transformation triggered successfully!")
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error triggering stock data transformation: ${e.message}")
        }
    }
}