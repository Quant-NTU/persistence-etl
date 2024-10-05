package sg.com.quantai.etl.requests

class BbcRawNewsArticleRequest(
    val title: String,
    val publishedDate: String,  // Use string for easier conversion
    val authors: String?,
    val description: String?,
    val section: String?,
    val content: String,
    val link: String,
    val topImage: String?
)