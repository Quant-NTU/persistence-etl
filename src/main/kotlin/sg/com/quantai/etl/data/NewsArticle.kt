package sg.com.quantai.etl.data

import org.springframework.data.annotation.Id
import org.bson.types.ObjectId
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDate
import java.time.LocalDateTime

@Document(collection = "news_articles")
data class NewsArticle(
    // Database columns
    @Id val _id: ObjectId = ObjectId.get(), // document id, it changes when updated via upsert
    // Columns
    val title: String,
    val publishedDate: LocalDate,
    val description: String,
    val content: String,
    // Timestamps
    val createdDate: LocalDateTime = LocalDateTime.now(),
    val updatedDate: LocalDateTime = LocalDateTime.now(),
)