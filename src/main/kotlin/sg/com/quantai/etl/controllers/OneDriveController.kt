package sg.com.quantai.etl.controllers

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.OneDriveDataService
import sg.com.quantai.etl.services.OneDriveDataProcessor

@RestController
@RequestMapping("/onedrive")
class OneDriveController(
    private val oneDriveDataService: OneDriveDataService,
    private val oneDriveDataProcessor: OneDriveDataProcessor
) {

    @PostMapping("/fetch")
    fun fetchAndStoreData(@RequestParam oneDriveUrl: String): ResponseEntity<String> {
        val extractedFiles = oneDriveDataService.downloadAndExtractData(oneDriveUrl)
        oneDriveDataProcessor.processCsvFiles(extractedFiles)
        return ResponseEntity.ok("Data fetched and stored successfully!")
    }
}
