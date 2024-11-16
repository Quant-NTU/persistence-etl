package sg.com.quantai.etl

import org.springframework.context.annotation.Bean
import org.springframework.web.client.RestTemplate
import io.github.cdimascio.dotenv.dotenv
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ETLApplication {

	@Bean
	fun restTemplate(): RestTemplate {
		return RestTemplate()
	}
}

fun main(args: Array<String>) {

	// Load environment variables from .env
	val dotenv = dotenv {
		ignoreIfMalformed = true // Ignore errors if the .env file is malformed
		ignoreIfMissing = true    // Ignore errors if the .env file is missing
	}

	// Access the API key from .env
	val cryptoCompareApiKey = dotenv["CRYPTOCOMPARE_API_KEY"]

	// Print API key to verify if it's loaded
	println("CryptoCompare API Key: $cryptoCompareApiKey")

	runApplication<ETLApplication>(*args)
}