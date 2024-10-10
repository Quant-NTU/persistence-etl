package sg.com.quantai.etl.data

import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDate

@Document(collection = "news_articles_bbc")
data class NewsArticleBBC(
    val title: String,
    val publishedDate: LocalDate,
    val authors: String?,
    val description: String?,
    val section: String?,
    val content: String,
    val link: String,
    val topImage: String?,

    @Id
    val _id: ObjectId = ObjectId.get()
)