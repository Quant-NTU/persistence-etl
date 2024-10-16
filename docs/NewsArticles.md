
# News Articles - ETL Pipeline 
This documentation outlines how the ETL microservice operates for fetching, transformation, and managing of news articles.
This microservice also interacts with the persistence-middleware, which will access the transformed news articles made available via this microservice and provide it to users (e.g researchers).

BBC News acts as the only source of news data at the time being, for this News Articles pipeline, hence this documentation is written mainly in the context of BBC news.

This microservice interacts with the external Hugging Face API to fetch, save, transform, and manage BBC news articles in MongoDB. A scheduler has been implemented to automate this entire pipeline, ensuring that the database is updated periodically with the latest historical news data available. 

## Fetching and Saving News Articles
### Controller Class: `NewsArticleController`

The `NewsArticleController` is responsible for fetching BBC news articles from Hugging Face's API and saving them into the MongoDB collections (`news_articles_bbc` and `news_articles`). This process includes deduplication based on article links to prevent redundant articles from being saved.


Naming conventions used are as follows:
- `news_articles` - denotes collection all transformed news articles from all sources (Note: BBC is the only source at the point of writing)
- `news_articles_bbc`- denotes collection for raw news articles, specific to BBC news

### Endpoints
#### For quick functionality testing
- To run initial fetching of raw data and loading into DB: `curl -X POST http://localhost:10070/news_articles/bbc/fetch`

- To run transformation pipeline and loading transf. rows into DB:  `curl -X POST http://localhost:10070/news_articles/bbc/transform`

- To retrieve all transformed news from database (within ETL):  `curl -X GET http://localhost:10070/news_articles/api/all`

- To retrieve all transformed BBC news from database (within middleware):  `curl -X GET http://localhost:10001/news_articles/all`

#### Fetch and Save BBC News Articles
- **Controller Method**: `NewsArticleController.fetchAndSaveBBCNews`
- **Endpoint**: `POST /news_articles/bbc/fetch`
- **Functionality**: Fetches news articles from the Hugging Face API and stores them in MongoDB.
- **Description**:
    - The service fetches articles from multiple months and saves them in the `news_articles_bbc` collection.
    - Deduplication is performed using the `link` field as the unique identifier. If an article with the same link already exists, it is not saved again.
- **Service:** This calls the `fetchAndSaveAllMonths` method in `NewsArticleBBCService`.

**Example Usage**:

```bash
POST /news_articles/bbc/fetch
Response: 200 OK
{
  "message": "News articles fetched and saved."
}
```

#### Transform BBC News Articles
- **Controller Method**: `NewsArticleController.transformBBC`
- **Endpoint**: `POST /news_articles/bbc/transform`
- **Functionality**: Transforms and saves BBC news articles from the raw collection (`news_articles_bbc`) to the transformed collection (`news_articles`).
- **Description**:
    - Filters for raw BBC news articles that have yet to be transformed, by retrieving them with `newsArticlesBBCRepository.findAllByTransformedFalse()`. This ensures that articles that have already been transformed do not undergo the transformation pipeline again and are not saved again into the transformed collection, eliminating duplication issues. 
    - Filters BBC news articles that have non-blank titles, descriptions, and content.
    - Transforms the data and stores it in the `news_articles` collection for further processing.
- **Service:** It calls the `transformAndSave` method in `NewsArticleService`.

**Example Usage**:

```bash
POST /news_articles/bbc/transform
Response: 200 OK
{
  "message": "Transformation completed successfully and results have been stored in the database."
}
```

#### Retrieve All News Articles

- **Endpoint**: `GET /news_articles/api/all`
- **Functionality**: Retrieves all transformed news articles from the `news_articles` MongoDB collection.
- **Controller**: `NewsArticleController.findAll`

**Example Usage**:

```bash
GET /news_articles/api/all
Response: 200 OK
[
  {
    "_id": "abc123",
    "title": "BBC News Article",
    "publishedDate": "2023-10-10",
    "description": "Detailed article description",
    "content": "Article content",
    "createdDate": "2023-10-11T10:00:00",
    "updatedDate": "2023-10-11T10:00:00"
  },
  ...
]
```

The middleware application will have access to this endpoint, being able to retrieve all the transformed news articles and then providing it to researchers who have access to the middleware.

--- 
## Data Models
Naming conventions used are as follows:
- `NewsArticle` - denotes data model for news articles from all sources (Note: BBC is the only source at the point of writing)
- `NewsArticleBBC`- denotes data model for raw news articles, specific to BBC 

### NewsArticle

Represents a transformed news article saved in the `news_articles` collection.

```kotlin
@Document(collection = "news_articles")
data class NewsArticle(
    @Id val _id: ObjectId = ObjectId.get(),
    val title: String,
    val publishedDate: LocalDate,
    val description: String,
    val content: String,
    val createdDate: LocalDateTime = LocalDateTime.now(),
    val updatedDate: LocalDateTime = LocalDateTime.now()
)
```

### NewsArticleBBC

Represents a raw BBC news article fetched from Hugging Face's API and saved in the `news_articles_bbc` collection.

```kotlin
@Document(collection = "news_articles_bbc")
data class NewsArticleBBC(
    @Id val _id: ObjectId = ObjectId.get(),
    val title: String,
    val publishedDate: LocalDate,
    val description: String?,
    val content: String,
    val link: String,
    val topImage: String?,
    var transformed: Boolean = false,

    val createdDate: LocalDateTime = LocalDateTime.now(),
    val updatedDate: LocalDateTime = LocalDateTime.now(),
) {
    @Transient var authors: String? = null;
    @Transient var section: String? = null;
}
```

---
## Services
Naming conventions used are as follows:
- `NewsArticleService`- denotes service that applies transformations on raw news articles, from all sources (Note: BBC is the only source at the point of writing)
- `NewsArticleBBCService` - denotes service that fetches raw BBC news articles and saves them into a database. This is specific to BBC.

### 1. `NewsArticleBBCService`

The `NewsArticleBBCService` handles the logic for fetching raw BBC news articles from the Hugging Face API, ensuring no duplicates are inserted, and saving the results into MongoDB.

#### Key Methods:

- **`fetchAndSaveAllMonths`**: This method fetches and saves articles for all months from the Hugging Face API, looping over each month to get data.

- **`fetchAndSaveForDate`**: A helper method that fetches and saves data for a specific month.

- **`saveNewsArticleBBC`**: This method saves individual news articles into the MongoDB collection, skipping articles that already exist by their link. The service also marks newly fetched articles with `transformed = false` to indicate that they are untransformed rows, ready to be fed into the transformation pipeline later.



### 2. `NewsArticleService`

The `NewsArticleService` is responsible for transforming raw news articles into a standardized format and saving them into a separate MongoDB collection.

#### Key Methods:

- **`transformAndSave`**: This method first retrieves the raw articles that are yet to be transformed. It then transforms raw news articles by filtering articles that have all the necessary fields and saving the transformed articles into the MongoDB collection.

### 3. `NewsArticleSchedulerService`

This service is responsible for scheduling periodic fetching and transformation of BBC news articles. It automates the process of calling the `NewsArticleBBCService` to fetch new raw articles and `NewsArticleService` to transform them.

- **Cron Schedule**: The service runs every month on the 15th at 2 AM.
- **Methods**:
  - `scheduleFetchAndTransformNews`: This method first triggers the fetch operation by calling `fetchAndSaveAllMonths` and then calls `transformAndSave` to process and store the transformed articles.
- **Logging**: The service logs the start and end of both the fetch and transformation processes for better traceability.
---

## Exception Handling

The service uses the `NewsArticleException` class to handle errors related to fetching, saving, or transforming news articles. This exception provides various constructors to support different use cases.

```kotlin
class NewsArticleException : RuntimeException {
    constructor() : super()
    constructor(message: String) : super(message)
    constructor(message: String, cause: Throwable) : super(message, cause)
    constructor(cause: Throwable) : super(cause)
}
```
---

## Repository Interfaces

### NewsArticleRepository

Interface for accessing the `news_articles` collection.

```kotlin
interface NewsArticleRepository : MongoRepository<NewsArticle, String> {}
```

### NewsArticleBBCRepository

Interface for accessing the `news_articles_bbc` collection. 
Includes methods to check for existing articles by their `link` field and to check for untransformed raw articles by their `transformed` field, to feed them into the transformation pipeline.

```kotlin
interface NewsArticleBBCRepository : MongoRepository<NewsArticleBBC, String> {
    fun existsByLink(link: String): Boolean
    fun countByLink(link: String): Long
    fun findAllByTransformedFalse(): List<NewsArticleBBC>
}
```
---

## Conclusion

This service is an integral part of the larger system for processing BBC news articles, providing a pipeline from raw data ingestion, transformation, and storage into the system for further use.
A scheduler service is also implemented to automate this entire pipeline as well, ensuring that the database is periodically updated with the latest historical news data available. 
