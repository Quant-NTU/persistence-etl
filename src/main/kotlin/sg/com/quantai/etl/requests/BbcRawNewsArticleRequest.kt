package sg.com.quantai.etl.requests

class BbcRawNewsArticleRequest(
    val title: String,
    val publishedDate: String,
    val authors: String?,
    val description: String?,
    val section: String?,
    val content: String,
    val link: String,
    val topImage: String?
)