package sg.com.quantai.etl.controllers

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.StockService
import sg.com.quantai.etl.services.StockTransformationService

@RestController
@RequestMapping("/stock")
class StockController(
    private val stockService: StockService,
    private val stockTransformationService: StockTransformationService
) {

    @PostMapping("/onedrive/fetch")
    fun fetchAndSaveStockFromOnedrive(): ResponseEntity<String> {
        return try {
            stockService.fetchAndSaveImportStockFromOneDrive()
            ResponseEntity("Stock data fetched from OneDrive and saved.", HttpStatus.OK)
        } catch (e: Exception) {
            ResponseEntity("Error triggering stock data import: ${e.message}", HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }

    @PostMapping("/onedrive/transform")
    fun transformStock(): ResponseEntity<String> {
        return try {
            stockTransformationService.transform()
            ResponseEntity("Stock data from OneDrive successfully transformed.", HttpStatus.OK)
        } catch (e: Exception) {
            ResponseEntity("Error triggering stock data transformation: ${e.message}", HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }
}