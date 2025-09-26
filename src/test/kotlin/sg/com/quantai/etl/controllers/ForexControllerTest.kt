package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import sg.com.quantai.etl.services.ForexService
import sg.com.quantai.etl.services.ForexTransformationService
import org.junit.jupiter.api.Assertions.assertEquals

class ForexControllerTest {

    private val forexService: ForexService = mock(ForexService::class.java)
    private val forexTransformationService: ForexTransformationService = mock(ForexTransformationService::class.java)
    private val controller = ForexController(forexService, forexTransformationService)

    @Test
    fun `getTopForexPairs returns OK with pairs`() {
        val mockPairs = listOf("EUR/USD", "GBP/USD", "USD/JPY")
        `when`(forexService.getTopForexPairs()).thenReturn(mockPairs)

        val response = controller.getTopForexPairs()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockPairs, response.body)
    }

    @Test
    fun `fetchAndStoreHistoricalData returns OK on success`() {
        doNothing().`when`(forexService).fetchAndStoreHistoricalData("EUR/USD", 30)

        val response = controller.fetchAndStoreHistoricalData("EUR/USD", 30)

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for EUR/USD fetched and stored successfully", response.body)
        verify(forexService, times(1)).fetchAndStoreHistoricalData("EUR/USD", 30)
    }

    @Test
    fun `fetchAndStoreHistoricalData returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(forexService.fetchAndStoreHistoricalData("EUR/USD", 30))
            .thenThrow(RuntimeException("API error"))

        val response = controller.fetchAndStoreHistoricalData("EUR/USD", 30)

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error fetching data for EUR/USD: API error", response.body)
    }

    @Test
    fun `fetchAndStoreHistoricalDataForTopPairs returns OK on success`() {
        doNothing().`when`(forexService).storeHistoricalDataForTopPairs()

        val response = controller.fetchAndStoreHistoricalDataForTopPairs()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Historical data for top forex pairs fetched and stored successfully", response.body)
        verify(forexService, times(1)).storeHistoricalDataForTopPairs()
    }

    @Test
    fun `fetchAndStoreHistoricalDataForTopPairs returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(forexService.storeHistoricalDataForTopPairs())
            .thenThrow(RuntimeException("Network error"))

        val response = controller.fetchAndStoreHistoricalDataForTopPairs()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error fetching top forex data: Network error", response.body)
    }

    @Test
    fun `triggerDataTransformation returns OK on success`() {
        doNothing().`when`(forexTransformationService).transformData()

        val response = controller.triggerDataTransformation()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Forex data transformation completed successfully", response.body)
        verify(forexTransformationService, times(1)).transformData()
    }

    @Test
    fun `triggerDataTransformation returns INTERNAL_SERVER_ERROR on exception`() {
        `when`(forexTransformationService.transformData())
            .thenThrow(RuntimeException("Database error"))

        val response = controller.triggerDataTransformation()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Error during forex data transformation: Database error", response.body)
    }
}

