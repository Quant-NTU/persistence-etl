package sg.com.quantai.etl.data

import java.time.LocalDateTime

data class StockData(
    val ticker: String,
    val date: LocalDateTime,
    val open: Double?,
    val high: Double?,
    val low: Double?,
    val close: Double?,
    val volume: Double?,
    val closeadj: Double?,
    val closeunadj: Double? = null,
    val lastUpdated: String? = null,
    val sourceFile: String
)