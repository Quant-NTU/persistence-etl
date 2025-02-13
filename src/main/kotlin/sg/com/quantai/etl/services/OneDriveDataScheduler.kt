package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/**
 * Scheduler component responsible for managing automated data fetching from OneDrive.
 * This component coordinates the download and processing of data files on a scheduled basis.
 */
@Component
class OneDriveDataScheduler(
    private val oneDriveDataService: OneDriveDataService,
    private val oneDriveDataProcessor: OneDriveDataProcessor
) {

    // Logger instance for this class
    private val logger: Logger = LoggerFactory.getLogger(OneDriveDataScheduler::class.java)

    // OneDrive URL containing the data files to be processed
    private val oneDriveUrl = "https://onedrive.live.com/?authkey=%21AJb7qigs6Jlt2NI&cid=4A43D63ABCBC9EA2&id=4A43D63ABCBC9EA2%211683&parId=4A43D63ABCBC9EA2%211679&o=OneUp"

    /**
     * Scheduled job that fetches and processes data from OneDrive.
     * Runs daily at 1:00 AM UTC.
     * 
     * The process:
     * 1. Downloads and extracts files from the configured OneDrive URL
     * 2. Processes the extracted CSV files using OneDriveDataProcessor
     * 3. Logs the start and completion of the process
     */
    @Scheduled(cron = "0 0 1 * * ?") // Runs daily at 1 AM UTC
    fun scheduleDataFetch() {
        logger.info("Scheduled OneDrive data fetch started...")
        val extractedFiles = oneDriveDataService.downloadAndExtractData(oneDriveUrl)
        oneDriveDataProcessor.processCsvFiles(extractedFiles)
        logger.info("Scheduled OneDrive data fetch completed.")
    }
}
