package sg.com.quantai.etl.controllers

import sg.com.quantai.etl.data.Test
import sg.com.quantai.etl.repositories.TestRepository

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/test")
class TestController {
    @Autowired
    val testRepository: TestRepository? = null

    @GetMapping("")
    fun getAllTests() : ResponseEntity<List<Test>> {
        // try {
        val tests: List<Test>? = testRepository?.findAll()

        return ResponseEntity<List<Test>>(tests, HttpStatus.OK)
        // } catch (e: Exception) {
        //     return ResponseEntity(null, HttpStatus.INTERNAL_SERVER_ERROR)
        // }
    }
}