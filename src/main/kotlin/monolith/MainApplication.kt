package monolith

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.SpringApplication

@SpringBootApplication
open class MainApplication

fun main(args: Array<String>) {
	SpringApplication.run(MainApplication::class.java, *args)
}