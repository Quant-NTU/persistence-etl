package sg.com.quantai.etl.data

import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.index.Indexed
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDate

@Document(collection = "bbc_news_articles_raw")
data class BbcRawNewsArticle(
    val title: String,
    val publishedDate: LocalDate,
    val authors: String?,
    val description: String?,
    val section: String?,
    val content: String,
    val link: String,
    val topImage: String?,

    @Indexed(unique = true)
    val uuid: String = ObjectId.get().toString(),

    @Id
    val _id: ObjectId = ObjectId.get() // MongoDB unique ID
)