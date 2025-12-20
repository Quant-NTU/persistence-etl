package sg.com.quantai.etl.controllers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.DataWarehouseETLService
import java.time.LocalDateTime

@RestController
@RequestMapping("/api/v1/warehouse")
class DataWarehouseController(
    private val dataWarehouseETLService: DataWarehouseETLService
) {

    private val logger: Logger = LoggerFactory.getLogger(DataWarehouseController::class.java)

    /**
     * Trigger a full load of data warehouse with optional parameters
     * POST /api/v1/warehouse/load/full
     * 
     * Query params:
     * - assetTypes: comma-separated list (CRYPTO,STOCK,FOREX) - default: all
     */
    @PostMapping("/load/full")
    fun loadFullWarehouse(
        @RequestParam(required = false) assetTypes: String?
    ): ResponseEntity<Map<String, Any>> {
        return try {
            val assetTypeList = assetTypes?.split(",")?.map { it.trim().uppercase() }?.filter { it.isNotEmpty() }
            
            logger.info("Triggering full data warehouse load for assets: ${assetTypeList ?: "ALL"}...")
            val result = dataWarehouseETLService.loadAllDataToWarehouse(assetTypeList)
            
            ResponseEntity.ok(mapOf(
                "status" to "success",
                "message" to "Full data warehouse load completed successfully",
                "data" to result
            ))
        } catch (e: Exception) {
            logger.error("Error during full warehouse load: ${e.message}", e)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mapOf(
                "status" to "error",
                "message" to "Failed to load data warehouse: ${e.message}"
            ))
        }
    }

    /**
     * Trigger an incremental load of data warehouse with optional parameters
     * POST /api/v1/warehouse/load/incremental
     * 
     * Query params:
     * - assetTypes: comma-separated list (CRYPTO,STOCK,FOREX) - default: all
     * - hoursBack: number of hours to look back (default: 2)
     */
    @PostMapping("/load/incremental")
    fun loadIncrementalWarehouse(
        @RequestParam(required = false) assetTypes: String?,
        @RequestParam(defaultValue = "2") hoursBack: Int
    ): ResponseEntity<Map<String, Any>> {
        return try {
            val assetTypeList = assetTypes?.split(",")?.map { it.trim().uppercase() }?.filter { it.isNotEmpty() }
            val effectiveHours = if (hoursBack < 1) 2 else if (hoursBack > 168) 168 else hoursBack // Max 1 week
            
            logger.info("Triggering incremental data warehouse load for assets: ${assetTypeList ?: "ALL"}, hours back: $effectiveHours...")
            val result = dataWarehouseETLService.incrementalLoadToWarehouse(assetTypeList, effectiveHours)
            
            ResponseEntity.ok(mapOf(
                "status" to "success",
                "message" to "Incremental data warehouse load completed successfully",
                "data" to result
            ))
        } catch (e: Exception) {
            logger.error("Error during incremental warehouse load: ${e.message}", e)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mapOf(
                "status" to "error",
                "message" to "Failed to load incremental data: ${e.message}"
            ))
        }
    }

    /**
     * Refresh materialized views
     * POST /api/v1/warehouse/refresh-views
     */
    @PostMapping("/refresh-views")
    fun refreshMaterializedViews(): ResponseEntity<Map<String, Any>> {
        return try {
            logger.info("Triggering materialized views refresh...")
            val result = dataWarehouseETLService.refreshMaterializedViews()
            
            ResponseEntity.ok(mapOf(
                "status" to "success",
                "message" to "Materialized views refreshed successfully",
                "data" to result
            ))
        } catch (e: Exception) {
            logger.error("Error refreshing materialized views: ${e.message}", e)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mapOf(
                "status" to "error",
                "message" to "Failed to refresh materialized views: ${e.message}"
            ))
        }
    }

    /**
     * Get data warehouse statistics
     * GET /api/v1/warehouse/statistics
     */
    @GetMapping("/statistics")
    fun getWarehouseStatistics(): ResponseEntity<Map<String, Any>> {
        return try {
            logger.info("Fetching data warehouse statistics...")
            val stats = dataWarehouseETLService.getWarehouseStatistics()
            
            ResponseEntity.ok(mapOf(
                "status" to "success",
                "data" to stats
            ))
        } catch (e: Exception) {
            logger.error("Error fetching warehouse statistics: ${e.message}", e)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mapOf(
                "status" to "error",
                "message" to "Failed to fetch statistics: ${e.message}"
            ))
        }
    }
}
