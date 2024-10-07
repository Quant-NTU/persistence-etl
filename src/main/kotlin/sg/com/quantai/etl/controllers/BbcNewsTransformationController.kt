package sg.com.quantai.etl.controllers
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import sg.com.quantai.etl.services.BbcNewsTransformationService
import org.springframework.http.ResponseEntity
import org.springframework.http.HttpStatus

@RestController
@RequestMapping("/transform_bbc")
class BbcNewsTransformationController(
    @Autowired private val bbcNewsTransformationService: BbcNewsTransformationService
) {

    @PostMapping("/run")
    fun runTransformation(): ResponseEntity<String> {
        try {
            bbcNewsTransformationService.transformAndSaveNewsArticles()
            return ResponseEntity("Transformation completed successfully and results have been stored in database.", HttpStatus.OK)
        } catch (ex: Exception) {
            return ResponseEntity("Transformation failed: ${ex.message}", HttpStatus.INTERNAL_SERVER_ERROR)
        }
    }
}
