package sg.com.quantai.etl.services

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import sg.com.quantai.etl.data.BbcRawNewsArticle
import sg.com.quantai.etl.data.BbcTransformedNewsArticle
import sg.com.quantai.etl.repositories.BbcRawNewsRepository
import sg.com.quantai.etl.repositories.BbcTransformedNewsRepository
import java.util.stream.Collectors

@Service
class BbcNewsTransformationService(
    @Autowired private val bbcRawNewsRepository: BbcRawNewsRepository,
    @Autowired private val bbcTransformedNewsRepository: BbcTransformedNewsRepository
) {
    fun transformAndSaveNewsArticles() {
        println("Transformation started.")
        val rawArticles: List<BbcRawNewsArticle> = bbcRawNewsRepository.findAll()

        val transformedArticles: List<BbcTransformedNewsArticle> = rawArticles.stream()
            .filter {
                it.title.isNotBlank() && it.description?.isNotBlank() == true && it.content.isNotBlank()
            }
            .map { rawArticle ->
                BbcTransformedNewsArticle(
                    title = rawArticle.title,
                    publishedDate = rawArticle.publishedDate,
                    description = rawArticle.description ?: "",
                    content = rawArticle.content
                )
            }.collect(Collectors.toList())

        bbcTransformedNewsRepository.saveAll(transformedArticles)

        println("Transformation completed. ${transformedArticles.size} transformed articles loaded into database.")
    }

}