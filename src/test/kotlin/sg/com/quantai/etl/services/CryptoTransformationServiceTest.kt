package sg.com.quantai.etl.services

import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.jdbc.core.JdbcTemplate
import sg.com.quantai.etl.services.cryptos.CryptoTransformationService

class CryptoTransformationServiceTest {

    private val jdbcTemplate: JdbcTemplate = Mockito.mock(JdbcTemplate::class.java)
    private val transformationService = CryptoTransformationService(jdbcTemplate)

    @Test
    fun `test data transformation`() {
        // Simulate successful data insertion
        Mockito.`when`(jdbcTemplate.update(Mockito.anyString())).thenReturn(5)

        transformationService.transformData()

        // Verify the update query was executed exactly once
        Mockito.verify(jdbcTemplate, Mockito.times(1)).update(Mockito.anyString())
    }
}