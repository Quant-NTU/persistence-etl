package sg.com.quantai.etl.controllers

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.bind.annotation.*
import org.springframework.http.ResponseEntity

@RestController
@RequestMapping("/nasdaq")
class NasdaqController(private val jdbcTemplate: JdbcTemplate) {

    @GetMapping("/data")
    fun getTransformedData(
        @RequestParam symbol: String?,
        @RequestParam startDate: String?,
        @RequestParam endDate: String?
    ): ResponseEntity<List<Map<String, Any>>> {
        val conditions = mutableListOf<String>()
        val params = mutableListOf<Any>()

        var sql = "SELECT * FROM transformed_nasdaq_data WHERE 1=1"

        symbol?.let {
            conditions.add("symbol = ?")
            params.add(it)
        }

        startDate?.let {
            conditions.add("date_time >= ?::timestamp")
            params.add(it)
        }

        endDate?.let {
            conditions.add("date_time <= ?::timestamp")
            params.add(it)
        }

        if (conditions.isNotEmpty()) {
            sql += " AND " + conditions.joinToString(" AND ")
        }

        sql += " ORDER BY date_time DESC"

        val result = jdbcTemplate.queryForList(sql, *params.toTypedArray())
        return ResponseEntity.ok(result)
    }
} 