package sg.com.quantai.etl

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ETLApplication

fun main(args: Array<String>) {
	runApplication<ETLApplication>(*args)
}