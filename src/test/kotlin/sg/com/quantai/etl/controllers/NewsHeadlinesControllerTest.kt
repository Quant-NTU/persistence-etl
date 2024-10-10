package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import sg.com.quantai.etl.data.BbcTransformedNewsArticle
import sg.com.quantai.etl.repositories.BbcTransformedNewsRepository
import sg.com.quantai.etl.services.BbcNewsApiService
import sg.com.quantai.etl.services.BbcNewsTransformationService
import org.junit.jupiter.api.Assertions.assertEquals
import java.time.LocalDate

class NewsHeadlinesControllerTest {

    @Mock
    private lateinit var bbcNewsTransformationService: BbcNewsTransformationService

    @Mock
    private lateinit var bbcNewsApiService: BbcNewsApiService

    @Mock
    private lateinit var bbcTransformedNewsRepository: BbcTransformedNewsRepository

    @InjectMocks
    private lateinit var newsHeadlinesController: NewsHeadlinesController

    @BeforeEach
    fun setUp() {
        MockitoAnnotations.openMocks(this)
    }

    @Test
    fun `fetchAndSaveNews should return OK status`() {
        val response: ResponseEntity<String> = newsHeadlinesController.fetchAndSaveNews()

        verify(bbcNewsApiService, times(1)).fetchAndSaveNewsArticlesForAllMonths()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("News articles fetched and saved.", response.body)
    }

    @Test
    fun `runTransformation should return OK when successful`() {
        doNothing().`when`(bbcNewsTransformationService).transformAndSaveNewsArticles()

        val response: ResponseEntity<String> = newsHeadlinesController.runTransformation()

        verify(bbcNewsTransformationService, times(1)).transformAndSaveNewsArticles()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Transformation completed successfully and results have been stored in database.", response.body)
    }

    @Test
    fun `runTransformation should return INTERNAL_SERVER_ERROR when transformation fails`() {
        doThrow(RuntimeException("Transformation failed")).`when`(bbcNewsTransformationService).transformAndSaveNewsArticles()

        val response: ResponseEntity<String> = newsHeadlinesController.runTransformation()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Transformation failed: Transformation failed", response.body)
    }

    @Test
    fun `getAllTransformedNews should return articles when available`() {
        val mockArticles = listOf(
            BbcTransformedNewsArticle("Title 1", LocalDate.parse("2023-10-01"), "Description 1", "Content 1"),
            BbcTransformedNewsArticle("Title 2", LocalDate.parse("2023-10-02"), "Description 2", "Content 2")
        )
        `when`(bbcTransformedNewsRepository.findAll()).thenReturn(mockArticles)

        val response: ResponseEntity<List<BbcTransformedNewsArticle>> = newsHeadlinesController.getAllTransformedNews()

        verify(bbcTransformedNewsRepository, times(1)).findAll()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockArticles, response.body)
    }

    @Test
    fun `getAllTransformedNews should return NO_CONTENT when no articles are available`() {
        `when`(bbcTransformedNewsRepository.findAll()).thenReturn(emptyList())

        val response: ResponseEntity<List<BbcTransformedNewsArticle>> = newsHeadlinesController.getAllTransformedNews()

        verify(bbcTransformedNewsRepository, times(1)).findAll()

        assertEquals(HttpStatus.NO_CONTENT, response.statusCode)
    }
}