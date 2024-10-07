package sg.com.quantai.etl.data

import org.springframework.data.annotation.Id
import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDate

@Document(collection = "bbc_news_articles_transformed")
data class BbcTransformedNewsArticle(
    val title: String,
    val publishedDate: LocalDate,
    val description: String,
    val content: String,

    @Id
    val _id: ObjectId = ObjectId.get()
)