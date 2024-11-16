package sg.com.quantai.etl.services

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate
import org.springframework.beans.factory.annotation.Value

@Service
class CryptoService(
    private val restTemplate: RestTemplate,
    @Value("\${CRYPTOCOMPARE_API_KEY}") val apiKey: String,
    private val objectMapper: ObjectMapper
) {

    fun fetchCryptoPrice(symbol: String, currency: String): JsonNode {
        val url = "https://min-api.cryptocompare.com/data/price?fsym=${symbol}&tsyms=${currency}&api_key=${apiKey}"
        val response = restTemplate.getForEntity(url, String::class.java)
        return objectMapper.readTree(response.body)
    }

    fun fetchHistoricalData(symbol: String, currency: String, limit: Int): JsonNode {
        val url = "https://min-api.cryptocompare.com/data/v2/histoday?fsym=${symbol}&tsym=${currency}&limit=${limit}&api_key=${apiKey}"
        val response = restTemplate.getForEntity(url, String::class.java)
        return objectMapper.readTree(response.body)
    }
}
