package sg.com.quantai.etl.exceptions

class NewsApiException(message: String, val statusCode: Int? = null, val details: String? = null) : RuntimeException(message) {
    override fun toString(): String {
        return "NewsApiException(message=$message, statusCode=$statusCode, details=$details)"
    }
}