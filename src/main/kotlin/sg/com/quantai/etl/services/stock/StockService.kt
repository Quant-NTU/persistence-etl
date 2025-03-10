package sg.com.quantai.etl.services.stock

import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.stereotype.Service
import java.io.File
import java.io.FileInputStream
import java.io.FileWriter
import java.sql.Connection
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import org.postgresql.core.BaseConnection
import sg.com.quantai.etl.services.stock.helpers.OneDriveService

@Service
class StockService(
    private val oneDriveService: OneDriveService,
    private val jdbcTemplate: JdbcTemplate,
    private val dataSource: DataSource
) {
    private val logger: Logger = LoggerFactory.getLogger(StockService::class.java)
    private val isProcessing = AtomicBoolean(false)
    private val executor: ExecutorService = Executors.newFixedThreadPool(4)

    private val dateFormatPatterns = listOf(
        "yyyy-MM-dd",
        "M/d/yyyy",
        "MM/dd/yyyy",
        "yyyy/MM/dd"
    )

    fun fetchAndSaveStockFromOneDrive() {
        if (isProcessing.getAndSet(true)) {
            logger.warn("Stock fetch and save is already in progress")
            return
        }

        try {
            logger.info("Starting stock fetch and save")

            val extractedFiles = oneDriveService.downloadAndExtractLatestFiles()
            if (extractedFiles.isEmpty()) {
                logger.warn("No files were extracted from OneDrive, skipping import")
                return
            }

            logger.info("Extracted files: $extractedFiles")
            extractedFiles.forEach { (fileType, filePath) ->
                try {
                    when (fileType) {
                        "SEP" -> processStockFile(filePath, fileType)
                        else -> logger.info("Skipping file type $fileType")
                    }
                } catch (e: Exception) {
                    logger.error("Error processing $fileType file: $filePath", e)
                }
            }

            oneDriveService.cleanupTempFiles()
            logger.info("Stock fetch and save completed successfully")
        } catch (e: Exception) {
            logger.error("Error during scheduled stock fetch and save", e)
        } finally {
            isProcessing.set(false)
        }
    }

    private fun processStockFile(filePath: String, fileType: String) {
        val file = File(filePath)
        if (!file.exists()) {
            logger.error("File does not exist: $filePath")
            return
        }

        logger.info("Processing $fileType file: $filePath")

        truncateRawStockData()

        when {
            filePath.endsWith(".xlsx", ignoreCase = true) -> processExcelFile(file)
            filePath.endsWith(".csv", ignoreCase = true) -> processCsvFile(file)
            else -> logger.warn("Unsupported file format: $filePath")
        }
    }

    private fun truncateRawStockData() {
        try {
            logger.info("Truncating raw_stock_nasdaq_data table before importing new data")
            jdbcTemplate.execute("TRUNCATE TABLE raw_stock_nasdaq_data")
            logger.info("Truncated raw_stock_nasdaq_data table")
        } catch (e: Exception) {
            logger.error("Error truncating raw_stock_nasdaq_data table: ${e.message}")
        }
    }

    private fun processExcelFile(file: File) {
        logger.info("Processing Excel file: ${file.name}")

        var validRowCount = 0
        var totalRowCount = 0

        try {
            FileInputStream(file).use { fis ->
                val workbook = WorkbookFactory.create(fis)
                val sheet = workbook.getSheetAt(0)
                val headerRow = sheet.getRow(0)
                val columnIndices = findColumnIndices(headerRow)

                if (!isOHLCVTable(columnIndices)) {
                    logger.warn("File ${file.name} does not appear to be an OHLCV table, skipping")
                    return
                }

                val tempCsvFile = File.createTempFile("stock_data_", ".csv")
                tempCsvFile.deleteOnExit()

                logger.info("Created temporary CSV file: ${tempCsvFile.absolutePath}")

                FileWriter(tempCsvFile).use { writer ->
                    for (i in 1..sheet.lastRowNum) {
                        val row = sheet.getRow(i) ?: continue
                        totalRowCount++

                        try {
                            val ticker = getCellValueAsString(row.getCell(columnIndices["ticker"] ?: -1)).uppercase()
                            val dateStr = getCellValueAsString(row.getCell(columnIndices["date"] ?: -1))
                            val date = parseDate(dateStr)

                            if (ticker.isBlank() || date == null) {
                                continue
                            }

                            val open = getCellValueAsDouble(row.getCell(columnIndices["open"] ?: -1))
                            val high = getCellValueAsDouble(row.getCell(columnIndices["high"] ?: -1))
                            val low = getCellValueAsDouble(row.getCell(columnIndices["low"] ?: -1))
                            val close = getCellValueAsDouble(row.getCell(columnIndices["close"] ?: -1))
                            val volume = getCellValueAsDouble(row.getCell(columnIndices["volume"] ?: -1))
                            val closeadj = getCellValueAsDouble(row.getCell(columnIndices["closeadj"] ?: -1))

                            writer.write("$ticker\t${Timestamp.valueOf(date)}\t${open ?: "\\N"}\t${high ?: "\\N"}\t${low ?: "\\N"}\t${close ?: "\\N"}\t${volume ?: "\\N"}\t${closeadj ?: "\\N"}\n")
                            validRowCount++
                        } catch (e: Exception) {
                            logger.warn("Error processing row $i: ${e.message}")
                        }
                    }
                }

                copyDataFromCsvFile(tempCsvFile)
                if (workbook is SXSSFWorkbook) {
                    workbook.dispose()
                }
                workbook.close()
                tempCsvFile.delete()

                logger.info("Processed Excel file: ${file.name}, valid rows: $validRowCount / $totalRowCount")
            }
        } catch (e: Exception) {
            logger.error("Error processing Excel file: ${file.name}", e)
            logger.info("Falling back to CSV processing")
            processCsvFile(file)
        }
    }

    private fun findColumnIndices(headerRow: Row?): Map<String, Int> {
        if (headerRow == null) return emptyMap()

        val indices = mutableMapOf<String, Int>()

        for (i in 0 until headerRow.lastCellNum) {
            val cell = headerRow.getCell(i) ?: continue
            val cellValue = cell.stringCellValue.trim().lowercase()

            when {
                cellValue == "ticker" -> indices["ticker"] = i
                cellValue == "date" -> indices["date"] = i
                cellValue == "open" -> indices["open"] = i
                cellValue == "high" -> indices["high"] = i
                cellValue == "low" -> indices["low"] = i
                cellValue == "close" -> indices["close"] = i
                cellValue == "volume" -> indices["volume"] = i
                cellValue == "closeadj" -> indices["closeadj"] = i
                cellValue == "closeunadj" -> indices["closeunadj"] = i
                cellValue == "lastupdated" -> indices["lastupdated"] = i
            }
        }

        logger.info("Found columns: ${indices.keys.joinToString(", ")}")

        return indices
    }

    private fun getCellValueAsString(cell: Cell?): String {
        if (cell == null) return ""

        return when (cell.cellType) {
            CellType.STRING -> cell.stringCellValue
            CellType.NUMERIC -> cell.numericCellValue.toString()
            CellType.BOOLEAN -> cell.booleanCellValue.toString()
            CellType.FORMULA -> {
                try {
                    cell.stringCellValue
                } catch (e: Exception) {
                    try {
                        cell.numericCellValue.toString()
                    } catch (e: Exception) {
                        ""
                    }
                }
            }
            else -> ""
        }
    }

    private fun getCellValueAsDouble(cell: Cell?): Double? {
        if (cell == null) return null

        return when (cell.cellType) {
            CellType.NUMERIC -> cell.numericCellValue
            CellType.STRING -> cell.stringCellValue.toDoubleOrNull()
            CellType.FORMULA -> {
                try {
                    cell.numericCellValue
                } catch (e: Exception) {
                    try {
                        cell.stringCellValue.toDoubleOrNull()
                    } catch (e: Exception) {
                        null
                    }
                }
            }
            else -> null
        }
    }

    private fun processCsvFile(file: File) {
        logger.info("Processing CSV file: ${file.name} (Size: ${file.length() / (1024 * 1024)} MB)")

        var validRowCount = 0
        var totalRowCount = 0
        val logFrequency = 100000 // Log progress every 100K rows
        val startTime = System.currentTimeMillis()

        try {
            val tempCsvFile = File.createTempFile("stock_data_", ".csv")
            tempCsvFile.deleteOnExit()

            logger.info("Created temporary CSV file: ${tempCsvFile.absolutePath}")

            file.bufferedReader().use { reader ->
                val headerLine = reader.readLine() ?: return
                val columnIndices = findCsvColumnIndices(headerLine)

                if (!isOHLCVTable(columnIndices)) {
                    logger.warn("File ${file.name} does not appear to be an OHLCV table, skipping")
                    return
                }

                FileWriter(tempCsvFile).use { writer ->
                    var line: String?
                    while (reader.readLine().also { line = it } != null) {
                        totalRowCount++

                        try {
                            val values = line?.split(",")?.map { it.trim() } ?: continue

                            if (values.size <= columnIndices.values.maxOrNull() ?: 0) {
                                continue
                            }

                            val ticker = values.getOrNull(columnIndices["ticker"] ?: continue)?.uppercase() ?: ""
                            val dateStr = values.getOrNull(columnIndices["date"] ?: continue) ?: ""
                            val date = parseDate(dateStr)

                            if (ticker.isBlank() || date == null) {
                                continue
                            }

                            val open = validateNumericOrNull(values.getOrNull(columnIndices["open"] ?: -1))
                            val high = validateNumericOrNull(values.getOrNull(columnIndices["high"] ?: -1))
                            val low = validateNumericOrNull(values.getOrNull(columnIndices["low"] ?: -1))
                            val close = validateNumericOrNull(values.getOrNull(columnIndices["close"] ?: -1))
                            val volume = validateNumericOrNull(values.getOrNull(columnIndices["volume"] ?: -1))
                            val closeadj = validateNumericOrNull(values.getOrNull(columnIndices["closeadj"] ?: -1))

                            writer.write("$ticker\t${Timestamp.valueOf(date)}\t${open ?: "\\\\N"}\t${high ?: "\\\\N"}\t${low ?: "\\\\N"}\t${close ?: "\\\\N"}\t${volume ?: "\\\\N"}\t${closeadj ?: "\\\\N"}\n")
                            validRowCount++

                            if (totalRowCount % logFrequency == 0) {
                                val currentTime = System.currentTimeMillis()
                                val elapsedSeconds = (currentTime - startTime) / 1000
                                val rowsPerSecond = if (elapsedSeconds > 0) totalRowCount / elapsedSeconds else 0
                                val memoryUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024)

                                logger.info("Progress: Processed $totalRowCount rows, Valid: $validRowCount rows " +
                                        "(${rowsPerSecond} rows/sec, Memory: $memoryUsed MB)")
                            }
                        } catch (e: Exception) {
                            if (totalRowCount % logFrequency == 0) {
                                logger.warn("Error processing CSV line $totalRowCount: ${e.message}")
                            }
                        }
                    }
                }
            }

            logger.info("CSV processing complete. Starting database COPY operation...")
            val copyStartTime = System.currentTimeMillis()

            copyDataFromCsvFile(tempCsvFile)

            val copyEndTime = System.currentTimeMillis()
            val copyDurationSeconds = (copyEndTime - copyStartTime) / 1000

            tempCsvFile.delete()

            val endTime = System.currentTimeMillis()
            val totalSeconds = (endTime - startTime) / 1000
            logger.info("Processed CSV file: ${file.name}, valid rows: $validRowCount / $totalRowCount " +
                    "in ${totalSeconds}s (CSV processing: ${totalSeconds - copyDurationSeconds}s, " +
                    "Database COPY: ${copyDurationSeconds}s)")
        } catch (e: Exception) {
            logger.error("Error processing CSV file: ${file.name}", e)
        }
    }

    private fun validateNumericOrNull(value: String?): String? {
        if (value == null || value.isBlank() || value.equals("null", ignoreCase = true) ||
            value.equals("n/a", ignoreCase = true) || value.equals("na", ignoreCase = true) ||
            value.equals("n", ignoreCase = true) || value == "-" || value == "--") {
            return null
        }

        return try {
            value.toDouble()
            value
        } catch (e: NumberFormatException) {
            null
        }
    }

    private fun copyDataFromCsvFile(csvFile: File) {
        logger.info("Starting PostgreSQL COPY operation with file size: ${csvFile.length() / (1024 * 1024)} MB")

        var connection: Connection? = null

        try {
            connection = DataSourceUtils.getConnection(dataSource)
            connection.autoCommit = false

            val copyManager = (connection.unwrap(BaseConnection::class.java)).copyAPI
            val sql = """
            COPY raw_stock_nasdaq_data (ticker, date, open, high, low, close, volume, closeadj)
            FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\t')
        """

            val startTime = System.currentTimeMillis()
            val records = csvFile.inputStream().use { inputStream ->
                copyManager.copyIn(sql, inputStream)
            }

            connection.commit()

            val endTime = System.currentTimeMillis()
            val durationSeconds = (endTime - startTime) / 1000
            val recordsPerSecond = if (durationSeconds > 0) records / durationSeconds else 0

            logger.info("Bulk loaded $records records in ${durationSeconds}s (${recordsPerSecond} records/sec) using PostgreSQL COPY command")
        } catch (e: Exception) {
            logger.error("Error during PostgreSQL COPY operation: ${e.message}", e)

            try {
                connection?.rollback()
            } catch (ex: SQLException) {
                logger.error("Error rolling back transaction: ${ex.message}")
            }

            if (e.message?.contains("invalid input syntax for type numeric", ignoreCase = true) == true) {
                logger.error("COPY failed due to invalid numeric data. Trying more aggressive data cleaning...")
                cleanCsvFileAndRetry(csvFile)
            } else {
                logger.info("Falling back to batch insert method")
                batchInsertFromCsvFile(csvFile)
            }
        } finally {
            try {
                connection?.autoCommit = true
                if (connection != null) {
                    DataSourceUtils.releaseConnection(connection, dataSource)
                }
            } catch (e: SQLException) {
                logger.error("Error resetting autoCommit: ${e.message}")
            }
        }
    }

    private fun cleanCsvFileAndRetry(originalCsvFile: File) {
        try {
            logger.info("Starting aggressive data cleaning of CSV file")

            val cleanedCsvFile = File.createTempFile("cleaned_stock_data_", ".csv")
            cleanedCsvFile.deleteOnExit()

            var processedLines = 0
            var cleanedLines = 0

            originalCsvFile.bufferedReader().use { reader ->
                FileWriter(cleanedCsvFile).use { writer ->
                    var line: String?
                    while (reader.readLine().also { line = it } != null) {
                        processedLines++

                        try {
                            val fields = line?.split('\t') ?: continue
                            if (fields.size < 9) continue

                            val ticker = fields[0]
                            val date = fields[1]

                            val cleanedFields = mutableListOf(ticker, date)

                            for (i in 2..7) {
                                val field = fields.getOrElse(i) { "\\N" }
                                val cleanedField = if (field == "\\N" || field.isEmpty()) {
                                    "\\N"
                                } else {
                                    try {
                                        field.toDouble()
                                        field // Valid number, keep as is
                                    } catch (e: NumberFormatException) {
                                        "\\N"
                                    }
                                }
                                cleanedFields.add(cleanedField)
                            }

                            cleanedFields.add(fields.getOrElse(8) { "" })

                            writer.write(cleanedFields.joinToString("\t") + "\n")
                            cleanedLines++

                            if (processedLines % 100000 == 0) {
                                logger.info("Cleaned $processedLines lines so far...")
                            }
                        } catch (e: Exception) {
                            logger.warn("Error cleaning line $processedLines: ${e.message}")
                        }
                    }
                }
            }

            logger.info("Cleaned CSV file created. Original lines: $processedLines, Cleaned lines: $cleanedLines")

            var connection: Connection? = null
            try {
                logger.info("Attempting COPY with cleaned data file")
                connection = DataSourceUtils.getConnection(dataSource)
                connection.autoCommit = false

                val copyManager = (connection.unwrap(BaseConnection::class.java)).copyAPI
                val sql = """
                COPY raw_stock_nasdaq_data (ticker, date, open, high, low, close, volume, closeadj)
                FROM STDIN WITH (FORMAT TEXT, NULL '\\N', DELIMITER E'\t')
            """

                val records = cleanedCsvFile.inputStream().use { inputStream ->
                    copyManager.copyIn(sql, inputStream)
                }

                connection.commit()
                logger.info("Successfully loaded $records records with cleaned data")
            } catch (e: Exception) {
                logger.error("Error during COPY with cleaned data: ${e.message}")
                try {
                    connection?.rollback()
                } catch (ex: SQLException) {
                    logger.error("Error rolling back transaction: ${ex.message}")
                }

                logger.info("Falling back to batch insert with cleaned data")
                batchInsertFromCsvFile(cleanedCsvFile)
            } finally {
                try {
                    connection?.autoCommit = true
                    if (connection != null) {
                        DataSourceUtils.releaseConnection(connection, dataSource)
                    }
                } catch (e: SQLException) {
                    logger.error("Error resetting autoCommit: ${e.message}")
                }

                cleanedCsvFile.delete()
            }
        } catch (e: Exception) {
            logger.error("Error during aggressive data cleaning: ${e.message}")
            logger.info("Falling back to batch insert with original data")
            batchInsertFromCsvFile(originalCsvFile)
        }
    }

    private fun batchInsertFromCsvFile(csvFile: File) {
        val batchSize = 5000 // Increased batch size
        val records = mutableListOf<Array<Any?>>()

        csvFile.bufferedReader().use { reader ->
            var line: String?
            while (reader.readLine().also { line = it } != null) {
                val values = line?.split("\t") ?: continue
                if (values.size < 8) continue

                // Create array with explicit Any? type
                val record = arrayOfNulls<Any?>(8).apply {
                    this[0] = values[0] // ticker
                    this[1] = values[1] // date - already in Timestamp format
                    this[2] = if (values[2] == "\\N") null else values[2].toDoubleOrNull() // open
                    this[3] = if (values[3] == "\\N") null else values[3].toDoubleOrNull() // high
                    this[4] = if (values[4] == "\\N") null else values[4].toDoubleOrNull() // low
                    this[5] = if (values[5] == "\\N") null else values[5].toDoubleOrNull() // close
                    this[6] = if (values[6] == "\\N") null else values[6].toDoubleOrNull() // volume
                    this[7] = if (values[7] == "\\N") null else values[7].toDoubleOrNull() // closeadj
                }

                records.add(record)

                if (records.size >= batchSize) {
                    executeBatchInsert(records)
                    records.clear()
                }
            }

            if (records.isNotEmpty()) {
                executeBatchInsert(records)
            }
        }
    }

    private fun executeBatchInsert(records: List<Array<Any?>>) {
        val sql = """
            INSERT INTO raw_stock_nasdaq_data (ticker, date, open, high, low, close, volume, closeadj) 
            VALUES (?, ?::timestamp, ?, ?, ?, ?, ?, ?)
        """

        var connection: Connection? = null
        try {
            connection = DataSourceUtils.getConnection(dataSource)
            connection.autoCommit = false

            connection.prepareStatement(sql).use { ps ->
                for (record in records) {
                    for (i in record.indices) {
                        ps.setObject(i + 1, record[i])
                    }
                    ps.addBatch()
                }
                ps.executeBatch()
            }

            connection.commit()
            logger.info("Inserted ${records.size} records using batch insert")
        } catch (e: Exception) {
            logger.error("Error executing batch insert: ${e.message}", e)
            connection?.let {
                try {
                    it.rollback()
                } catch (ex: SQLException) {
                    logger.error("Error rolling back transaction: ${ex.message}")
                }
            }
        } finally {
            connection?.let {
                try {
                    it.autoCommit = true
                    DataSourceUtils.releaseConnection(it, dataSource)
                } catch (e: SQLException) {
                    logger.error("Error resetting autoCommit: ${e.message}")
                }
            }
        }
    }

    private fun findCsvColumnIndices(headerLine: String): Map<String, Int> {
        val indices = mutableMapOf<String, Int>()
        val headers = headerLine.split(",").map { it.trim().lowercase() }

        headers.forEachIndexed { index, header ->
            when {
                header == "ticker" -> indices["ticker"] = index
                header == "date" -> indices["date"] = index
                header == "open" -> indices["open"] = index
                header == "high" -> indices["high"] = index
                header == "low" -> indices["low"] = index
                header == "close" -> indices["close"] = index
                header == "volume" -> indices["volume"] = index
                header == "closeadj" -> indices["closeadj"] = index
                header == "closeunadj" -> indices["closeunadj"] = index
                header == "lastupdated" -> indices["lastupdated"] = index
            }
        }

        logger.info("Found CSV columns: ${indices.keys.joinToString(", ")}")

        return indices
    }

    private fun isOHLCVTable(columnIndices: Map<String, Int>): Boolean {
        val mandatoryColumns = listOf("ticker", "date")
        val priceVolumeColumns = listOf("open", "high", "low", "close", "volume", "closeadj")

        val hasMandatory = mandatoryColumns.all { columnIndices.containsKey(it) }
        val hasSomePriceVolume = priceVolumeColumns.any { columnIndices.containsKey(it) }

        logger.info("Table check - has mandatory columns: $hasMandatory, has price/volume columns: $hasSomePriceVolume")

        return hasMandatory && hasSomePriceVolume
    }

    private fun parseDate(dateStr: String): LocalDateTime? {
        if (dateStr.isBlank()) return null

        for (pattern in dateFormatPatterns) {
            try {
                val formatter = DateTimeFormatter.ofPattern(pattern)
                val date = LocalDate.parse(dateStr, formatter)
                return date.atStartOfDay()
            } catch (e: DateTimeParseException) {
                // Try next pattern
            }
        }

        try {
            return LocalDateTime.parse(dateStr)
        } catch (e: DateTimeParseException) {
            // If still failing, try a few more specific formats
            try {
                val numericDate = dateStr.toDoubleOrNull()
                if (numericDate != null) {
                    val javaDate = LocalDateTime.of(1899, 12, 30, 0, 0).plusDays(numericDate.toLong())
                    return javaDate
                }
            } catch (e: Exception) {
                logger.warn("Failed to parse date from numeric value: $dateStr")
            }
        }

        logger.warn("Unable to parse date string: '$dateStr'")
        return null
    }
}