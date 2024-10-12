package sg.com.quantai.etl.data

import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDate
import java.time.LocalDateTime

@Document(collection = "news_articles_bbc")
data class NewsArticleBBC(
    // Database columns
    @Id val _id: ObjectId = ObjectId.get(), // document id, it changes when updated via upsert
    // Columns
    val title: String,
    val publishedDate: LocalDate,
    val description: String?,
    val content: String,
    val link: String,
    val topImage: String?,
    // Timestamps
    val createdDate: LocalDateTime = LocalDateTime.now(),
    val updatedDate: LocalDateTime = LocalDateTime.now(),
) {
    @Transient var authors: String? = null;
    @Transient var section: String? = null;
}