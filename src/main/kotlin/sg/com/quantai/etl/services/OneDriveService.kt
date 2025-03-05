package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.ZipInputStream
import kotlin.io.path.createDirectories
import java.net.HttpURLConnection

@Service
class OneDriveService(
    private val webClientBuilder: WebClient.Builder,
    private val objectMapper: ObjectMapper,
    @Value("\${quantai.onedrive.folder.url:https://1drv.ms/f/s!AvYU2LBfd0QYgYBCtbRmcfcayPDXsA?e=x5bnA0}")
    private val oneDriveFolderUrl: String
) {
    private val logger: Logger = LoggerFactory.getLogger(OneDriveService::class.java)
    private val isProcessing = AtomicBoolean(false)
    private val tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "quantai-etl")

    private val webClient: WebClient by lazy {
        webClientBuilder.build()
    }

    fun downloadAndExtractLatestFiles(): Map<String, String> {
        if (isProcessing.getAndSet(true)) {
            logger.warn("File download and extraction is already in progress")
            return emptyMap()
        }

        try {
            logger.info("Starting download from OneDrive shared folder: $oneDriveFolderUrl")

            // Prepare temporary directory
            tempDir.createDirectories()

            // Create the share ID for OneDrive API
            val shareId = createShareId(oneDriveFolderUrl)
            logger.info("Generated share ID: $shareId")

            // Query the OneDrive API for folder contents
            val filesList = getFilesFromSharedFolder(shareId)
            if (filesList.isEmpty()) {
                logger.warn("No files found in shared folder")
                return emptyMap()
            }

            logger.info("Found ${filesList.size} files in shared folder")

            // Find the latest SEP file
            val sepFile = filesList.filter { it.name.contains("SEP", ignoreCase = true) }
                .maxByOrNull { it.lastModified }

            if (sepFile == null) {
                logger.warn("No SEP files found in the folder")
                return emptyMap()
            }

            logger.info("Found SEP file: ${sepFile.name}, last modified: ${sepFile.lastModified}")

            // Download and process the file
            val extractedFilePath = downloadAndExtractFile(sepFile)

            val result = mutableMapOf<String, String>()
            if (extractedFilePath != null) {
                result["SEP"] = extractedFilePath
                logger.info("Successfully extracted SEP file to: $extractedFilePath")
            }

            return result
        } catch (e: Exception) {
            logger.error("Error during file download and extraction", e)
            return emptyMap()
        } finally {
            isProcessing.set(false)
        }
    }

    /**
     * Creates a share ID for the OneDrive API from a sharing link
     */
    private fun createShareId(sharingLink: String): String {
        // Base64 encode the sharing link
        val encodedLink = Base64.getEncoder().encodeToString(sharingLink.toByteArray())

        // Create the share ID by replacing characters as per OneDrive API requirements
        return "u!" + encodedLink.replace("/", "_").replace("+", "-")
    }

    /**
     * Gets files list from a shared OneDrive folder using the share ID
     */
    private fun getFilesFromSharedFolder(shareId: String): List<OneDriveFileInfo> {
        try {
            // Construct the OneDrive API URL
            val apiUrl = "https://api.onedrive.com/v1.0/shares/$shareId/driveItem?${'$'}expand=children"

            logger.info("Querying OneDrive API: $apiUrl")

            val response = webClient.get()
                .uri(apiUrl)
                .retrieve()
                .bodyToMono(String::class.java)
                .doOnError { e ->
                    if (e is WebClientResponseException) {
                        logger.error("API Error: Status ${e.statusCode}, Body: ${e.responseBodyAsString}")
                    } else {
                        logger.error("API Error: ${e.message}")
                    }
                }
                .block() ?: return emptyList()

            val jsonNode = objectMapper.readTree(response)
            val childrenNode = jsonNode.path("children")

            if (childrenNode.isMissingNode() || !childrenNode.isArray()) {
                logger.warn("API response did not contain children listing")
                logger.info("Full response: $response")
                return emptyList()
            }

            val filesList = mutableListOf<OneDriveFileInfo>()

            for (item in childrenNode) {
                val name = item.path("name").asText("")
                if (name.isBlank()) continue

                // Only process ZIP files
                if (!name.endsWith(".zip", ignoreCase = true)) continue

                val lastModifiedStr = item.path("lastModifiedDateTime").asText("")

                // For download URL, we need to check the @microsoft.graph.downloadUrl property
                // If not available, we can construct one from the ID
                val downloadUrl = item.path("@microsoft.graph.downloadUrl").asText("")
                    .ifBlank {
                        // Fallback to constructing URL from ID
                        val id = item.path("id").asText("")
                        if (id.isNotBlank()) {
                            "https://api.onedrive.com/v1.0/shares/$shareId/items/$id/content"
                        } else ""
                    }

                if (lastModifiedStr.isBlank() || downloadUrl.isBlank()) {
                    logger.warn("Missing properties for file: $name")
                    continue
                }

                try {
                    // Parse the date (format may vary)
                    val lastModified = if (lastModifiedStr.contains("T")) {
                        // ISO format like 2023-04-01T12:34:56Z
                        LocalDateTime.parse(lastModifiedStr.substringBefore("Z"))
                    } else {
                        // Simple date format
                        LocalDateTime.parse(lastModifiedStr)
                    }

                    filesList.add(
                        OneDriveFileInfo(
                            name = name,
                            lastModified = lastModified,
                            downloadUrl = downloadUrl
                        )
                    )

                    logger.info("Found file: $name, last modified: $lastModified")
                } catch (e: Exception) {
                    logger.warn("Error parsing metadata for file $name: ${e.message}")
                }
            }

            return filesList
        } catch (e: Exception) {
            logger.error("Error getting files from shared folder: ${e.message}", e)
            return emptyList()
        }
    }

    /**
     * Downloads and extracts a file
     */
    private fun downloadAndExtractFile(fileInfo: OneDriveFileInfo): String? {
        try {
            logger.info("Downloading file: ${fileInfo.name} from URL: ${fileInfo.downloadUrl}")

            // Create a temporary file for the zip content
            val zipFile = Files.createTempFile(tempDir, "download-", ".zip")

            // Use HttpURLConnection which handles redirects better for large files
            val connection = URL(fileInfo.downloadUrl).openConnection() as HttpURLConnection
            connection.instanceFollowRedirects = true
            connection.connectTimeout = 60000  // 60 seconds
            connection.readTimeout = 3600000   // 1 hour
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

            val responseCode = connection.responseCode
            if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.error("Download failed with response code: $responseCode")
                if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP ||
                    responseCode == HttpURLConnection.HTTP_MOVED_PERM) {
                    // Handle redirect manually if needed
                    val newUrl = connection.getHeaderField("Location")
                    logger.info("Following redirect to: $newUrl")
                    return downloadAndExtractFile(fileInfo.copy(downloadUrl = newUrl))
                }
                return null
            }

            // Get content length if available
            val contentLength = connection.contentLength
            val contentLengthMB = if (contentLength > 0) contentLength / (1024 * 1024) else "unknown"
            logger.info("Starting download of $contentLengthMB MB file")

            // Get input stream and write to file in chunks
            connection.inputStream.use { input ->
                Files.newOutputStream(zipFile).use { output ->
                    val buffer = ByteArray(8192)
                    var bytesRead: Int
                    var totalBytesRead = 0L
                    val startTime = System.currentTimeMillis()

                    while (input.read(buffer).also { bytesRead = it } != -1) {
                        output.write(buffer, 0, bytesRead)
                        totalBytesRead += bytesRead

                        // Log progress every 10MB
                        if (totalBytesRead % (10 * 1024 * 1024) < 8192) {
                            val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000
                            val mbDownloaded = totalBytesRead / (1024 * 1024)
                            logger.info("Downloaded $mbDownloaded MB in $elapsedSeconds seconds")
                        }
                    }

                    val totalMB = totalBytesRead / (1024 * 1024)
                    val totalTime = (System.currentTimeMillis() - startTime) / 1000
                    logger.info("Completed download: $totalMB MB in $totalTime seconds")
                }
            }

            // Check file size
            val fileSize = Files.size(zipFile)
            if (fileSize == 0L) {
                logger.error("Downloaded file is empty")
                Files.deleteIfExists(zipFile)
                return null
            }

            logger.info("Successfully downloaded ${fileSize / (1024 * 1024)} MB to: $zipFile")

            // Extract the content
            val extractedFilePath = extractZipFile(zipFile, fileInfo.name)

            // Clean up the zip file
            Files.deleteIfExists(zipFile)

            return extractedFilePath
        } catch (e: Exception) {
            logger.error("Error downloading or extracting file: ${fileInfo.name}", e)
            e.printStackTrace() // Add full stack trace for debugging
            return null
        }
    }

    /**
     * Extracts a zip file and returns the path to the extracted Excel file
     */
    private fun extractZipFile(zipFilePath: Path, originalFileName: String): String? {
        val targetDir = tempDir.resolve(originalFileName.substringBeforeLast('.'))
        targetDir.createDirectories()

        return try {
            logger.info("Extracting zip file to: $targetDir")

            ZipInputStream(ByteArrayInputStream(Files.readAllBytes(zipFilePath))).use { zipInputStream ->
                var entry = zipInputStream.nextEntry
                while (entry != null) {
                    val entryName = entry.name

                    if (!entry.isDirectory && (entryName.endsWith(".xlsx") || entryName.endsWith(".csv"))) {
                        val outputFile = targetDir.resolve(entryName).toString()
                        val outputPath = File(outputFile).toPath()

                        // Create parent directories if needed
                        outputPath.parent?.createDirectories()

                        // Extract the file
                        FileOutputStream(outputFile).use { output ->
                            val buffer = ByteArray(8192)
                            var bytesRead: Int
                            var totalBytesRead = 0L
                            val startTime = System.currentTimeMillis()

                            while (zipInputStream.read(buffer).also { bytesRead = it } != -1) {
                                output.write(buffer, 0, bytesRead)
                                totalBytesRead += bytesRead

                                // Log progress every 10MB
                                if (totalBytesRead % (10 * 1024 * 1024) < 8192) {
                                    val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000
                                    val mbExtracted = totalBytesRead / (1024 * 1024)
                                    logger.info("Extracted $mbExtracted MB in $elapsedSeconds seconds")
                                }
                            }

                            val totalMB = totalBytesRead / (1024 * 1024)
                            val totalTime = (System.currentTimeMillis() - startTime) / 1000
                            logger.info("Completed extraction: $totalMB MB in $totalTime seconds")
                        }

                        logger.info("Extracted file: $outputFile")
                        return outputFile
                    }

                    zipInputStream.closeEntry()
                    entry = zipInputStream.nextEntry
                }

                logger.warn("No Excel or CSV file found in the zip archive")
                null
            }
        } catch (e: Exception) {
            logger.error("Error extracting zip file", e)
            null
        }
    }

    /**
     * Cleans up temporary files older than the specified number of days
     */
    fun cleanupTempFiles(olderThanDays: Int = 1) {
        try {
            logger.info("Cleaning up temporary files older than $olderThanDays days")

            val cutoffTime = LocalDateTime.now().minusDays(olderThanDays.toLong())

            Files.walk(tempDir)
                .filter { path ->
                    Files.isRegularFile(path) ||
                            (Files.isDirectory(path) && !path.equals(tempDir))
                }
                .filter { path ->
                    val lastModified = LocalDateTime.ofInstant(
                        Files.getLastModifiedTime(path).toInstant(),
                        ZoneOffset.UTC
                    )
                    lastModified.isBefore(cutoffTime)
                }
                .forEach { path ->
                    try {
                        if (Files.isDirectory(path)) {
                            Files.walk(path)
                                .sorted(Comparator.reverseOrder())
                                .forEach { Files.deleteIfExists(it) }
                        } else {
                            Files.deleteIfExists(path)
                        }
                    } catch (e: Exception) {
                        logger.warn("Failed to delete path: $path", e)
                    }
                }

            logger.info("Temporary file cleanup completed")
        } catch (e: Exception) {
            logger.error("Error during temporary file cleanup", e)
        }
    }

    /**
     * Data class to store file information
     */
    data class OneDriveFileInfo(
        val name: String,
        val lastModified: LocalDateTime,
        val downloadUrl: String
    )
}