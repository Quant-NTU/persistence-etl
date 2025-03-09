package sg.com.quantai.etl.controllers

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.StockDataService
import sg.com.quantai.etl.services.StockDataTransformationService

@RestController
@RequestMapping("/stock")
class StockDataController(
    private val stockDataService: StockDataService,
    private val transformationService: StockDataTransformationService
) {

    @PostMapping("/onedrive/fetch")
    fun fetchAndSaveStockDataFromOnedrive(): ResponseEntity<String> {
        return try {
            stockDataService.importStockDataFromOneDrive()
            ResponseEntity("Stock data fetched from OneDrive and saved.", HttpStatus.OK)
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error triggering stock data import: ${e.message}")
        }
    }

    @PostMapping("/onedrive/transform")
    fun transformStockData(): ResponseEntity<String> {
        return try {
            transformationService.manuallyTriggerTransformation()
            ResponseEntity("Stock data from OneDrive successfully transformed.", HttpStatus.OK)
        } catch (e: Exception) {
            ResponseEntity.status(500).body("Error triggering stock data transformation: ${e.message}")
        }
    }
}