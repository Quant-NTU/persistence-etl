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
        @RequestParam(defaultValue = "30") outputsize: Int
    ): ResponseEntity<String> {
        return try {
            forexService.fetchAndStoreHistoricalData(currencyPair, outputsize)
            ResponseEntity.ok("Historical data for $currencyPair fetched and stored successfully")
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body("Error fetching data for $currencyPair: ${e.message}")
        }
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
}
