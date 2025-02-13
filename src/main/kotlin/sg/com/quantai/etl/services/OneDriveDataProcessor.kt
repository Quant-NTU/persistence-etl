package sg.com.quantai.etl.services

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.io.File

/**
 * Service class responsible for processing CSV files containing NASDAQ stock data
 * and storing it in a PostgreSQL database.
 */
@Service
class OneDriveDataProcessor(private val jdbcTemplate: JdbcTemplate) {

    // Logger instance for this class
    private val logger: Logger = LoggerFactory.getLogger(OneDriveDataProcessor::class.java)

    /**
     * Process a list of CSV files containing NASDAQ stock data
     * Creates the database table if it doesn't exist and inserts data from each file
     * @param files List of CSV files to process
     */
    fun processCsvFiles(files: List<File>) {
        // Create table if not exists
        createTableIfNotExists()
        
        files.forEach { file ->
            logger.info("Processing NASDAQ CSV file: ${file.name}")
            file.bufferedReader().useLines { lines ->
                lines.drop(1).forEach { line -> // Skip header row
                    val data = line.split(",")
                    if (data.size >= 7) { // Ensure we have all required fields: symbol,date,open,high,low,close,volume
                        insertDataIntoDB(data)
                    }
                }
            }
        }
    }

    /**
     * Creates the raw_nasdaq_data table if it doesn't exist in the database
     * Table structure includes columns for stock symbol, datetime, and OHLCV data
     */
    private fun createTableIfNotExists() {
        val sql = """
            CREATE TABLE IF NOT EXISTS raw_nasdaq_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date_time TIMESTAMP WITH TIME ZONE NOT NULL,
                open DECIMAL NOT NULL,
                high DECIMAL NOT NULL,
                low DECIMAL NOT NULL,
                close DECIMAL NOT NULL,
                volume DECIMAL NOT NULL
            )
        """
        jdbcTemplate.execute(sql)
    }

    /**
     * Inserts a single row of NASDAQ stock data into the database
     * @param data List of strings containing the stock data in order:
     *             [symbol, date, open, high, low, close, volume]
     */
    private fun insertDataIntoDB(data: List<String>) {
        try {
            val sql = """
                INSERT INTO raw_nasdaq_data (symbol, date_time, open, high, low, close, volume)
                VALUES (?, ?::timestamp, ?, ?, ?, ?, ?)
            """
            jdbcTemplate.update(sql, 
                data[0], // symbol
                data[1], // date
                data[2].toDouble(), // open price
                data[3].toDouble(), // high price
                data[4].toDouble(), // low price
                data[5].toDouble(), // close price
                data[6].toDouble()  // trading volume
            )
        } catch (e: Exception) {
            logger.error("Error inserting NASDAQ data into DB: ${e.message}")
        }
    }
}
