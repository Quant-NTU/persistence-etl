package sg.com.quantai.etl.repositories

import sg.com.quantai.etl.data.NewsArticleBBC

import org.springframework.data.mongodb.repository.MongoRepository

interface NewsArticleBBCRepository : MongoRepository<NewsArticleBBC, String> {}