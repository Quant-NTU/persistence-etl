package sg.com.quantai.etl.repositories

import sg.com.quantai.etl.data.BbcRawNewsArticle
import org.springframework.data.mongodb.repository.MongoRepository

interface BbcRawNewsRepository : MongoRepository<BbcRawNewsArticle, String> {
    fun findOneByUuid(uuid: String): BbcRawNewsArticle
}