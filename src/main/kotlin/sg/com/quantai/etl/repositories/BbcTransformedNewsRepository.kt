package sg.com.quantai.etl.repositories

import sg.com.quantai.etl.data.BbcTransformedNewsArticle
import org.springframework.data.mongodb.repository.MongoRepository

interface BbcTransformedNewsRepository : MongoRepository<BbcTransformedNewsArticle, String> {
}