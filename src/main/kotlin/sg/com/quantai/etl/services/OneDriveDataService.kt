package sg.com.quantai.etl.services

import org.apache.commons.io.FileUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.File
import java.net.URL
import java.nio.file.Files
import java.util.zip.ZipFile

/**
 * Service class responsible for downloading and extracting data files from OneDrive.
 * This service handles downloading ZIP files from OneDrive and extracting their contents
 * to be processed by other services.
 */
@Service
class OneDriveDataService {

    // Logger instance for this class
    private val logger: Logger = LoggerFactory.getLogger(OneDriveDataService::class.java)
    
    // Directory paths for downloaded and extracted files
    private val downloadDir = "data/downloads"
    private val extractDir = "data/extracted"

    /**
     * Downloads a ZIP file from OneDrive and extracts its contents.
     * 
     * @param oneDriveUrl The URL of the ZIP file on OneDrive to download
     * @return List of extracted files, or empty list if download/extraction fails
     * 
     * The process:
     * 1. Downloads ZIP file from provided OneDrive URL
     * 2. Saves it to the downloads directory
     * 3. Extracts contents to extraction directory
     * 4. Returns list of extracted files
     */
    fun downloadAndExtractData(oneDriveUrl: String): List<File> {
        logger.info("Downloading ZIP file from OneDrive: $oneDriveUrl")

        val zipFilePath = "$downloadDir/data.zip"
        val zipFile = File(zipFilePath)

        try {
            FileUtils.copyURLToFile(URL(oneDriveUrl), zipFile)
            logger.info("ZIP file downloaded successfully: $zipFilePath")

            return extractZip(zipFile)
        } catch (e: Exception) {
            logger.error("Failed to download ZIP file: ${e.message}")
            return emptyList()
        }
    }

    /**
     * Extracts contents of a ZIP file and returns list of extracted files.
     * 
     * @param zipFile The ZIP file to extract
     * @return List of extracted files
     * 
     * The extraction process:
     * 1. Creates extraction directory if it doesn't exist
     * 2. Iterates through ZIP file entries
     * 3. Creates directories for directory entries
     * 4. Extracts files for file entries
     * 5. Returns list of all extracted files
     */
    private fun extractZip(zipFile: File): List<File> {
        logger.info("Extracting ZIP file: ${zipFile.absolutePath}")

        val extractedFiles = mutableListOf<File>()
        val extractDirFile = File(extractDir)
        if (!extractDirFile.exists()) extractDirFile.mkdirs()

        ZipFile(zipFile).use { zip ->
            zip.entries().asSequence().forEach { entry ->
                val extractedFile = File(extractDir, entry.name)
                if (entry.isDirectory) {
                    extractedFile.mkdirs()
                } else {
                    Files.copy(zip.getInputStream(entry), extractedFile.toPath())
                    extractedFiles.add(extractedFile)
                }
            }
        }

        logger.info("ZIP extraction completed. Extracted files: ${extractedFiles.map { it.name }}")
        return extractedFiles
    }
}
