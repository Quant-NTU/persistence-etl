package monolith

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class GreeterController {

    @GetMapping("/greeting")
    fun greeting() = ResponseEntity.ok("Hello")
    
}