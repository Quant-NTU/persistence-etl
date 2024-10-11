package sg.com.quantai.etl.repositories

import sg.com.quantai.etl.data.NewsArticle

import org.springframework.data.mongodb.repository.MongoRepository

interface NewsArticleRepository : MongoRepository<NewsArticle, String> {}