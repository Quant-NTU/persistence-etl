package sg.com.quantai.etl.controllers

import org.junit.jupiter.api.Test
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.*
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.repositories.NewsArticlesRepository
import sg.com.quantai.etl.services.NewsArticlesBBCService
import sg.com.quantai.etl.services.NewsArticlesService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.test.context.junit.jupiter.SpringExtension
import java.time.LocalDate

@ExtendWith(SpringExtension::class)
class NewsArticlesControllerTest {

    @Mock
    private lateinit var newsArticlesService: NewsArticlesService

    @Mock
    private lateinit var newsArticlesBBCService: NewsArticlesBBCService

    @Mock
    private lateinit var newsArticlesRepository: NewsArticlesRepository

    @InjectMocks
    private lateinit var newsArticlesController: NewsArticlesController

    @Test
    fun `Should connect to fetch results`() {
        val response: ResponseEntity<String> = newsArticlesController.fetchAndSaveNews()

        verify(newsArticlesBBCService, times(1)).fetchAndSaveNewsArticlesForAllMonths()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("News articles fetched and saved.", response.body)
    }

    @Test
    fun `Should apply transformations on BBC data and save results`() {
        doNothing().`when`(newsArticlesService).transformAndSaveNewsArticles()

        val response: ResponseEntity<String> = newsArticlesController.runTransformation()

        verify(newsArticlesService, times(1)).transformAndSaveNewsArticles()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Transformation completed successfully and results have been stored in database.", response.body)
    }

    @Test
    fun `Should yield proper exception if a transformation fails`() {
        doThrow(RuntimeException("Transformation failed")).`when`(newsArticlesService).transformAndSaveNewsArticles()

        val response: ResponseEntity<String> = newsArticlesController.runTransformation()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Transformation failed: Transformation failed", response.body)
    }

    @Test
    fun `Should yield transformed articles if at least one is available`() {
        val mockArticles = listOf(
            NewsArticle("Title 1", LocalDate.parse("2023-10-01"), "Description 1", "Content 1"),
            NewsArticle("Title 2", LocalDate.parse("2023-10-02"), "Description 2", "Content 2")
        )
        `when`(newsArticlesRepository.findAll()).thenReturn(mockArticles)

        val response: ResponseEntity<List<NewsArticle>> = newsArticlesController.getAllTransformedNews()

        verify(newsArticlesRepository, times(1)).findAll()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockArticles, response.body)
    }

    @Test
    fun `A NO_CONTENT response should be given if no articles are available`() {
        `when`(newsArticlesRepository.findAll()).thenReturn(emptyList())

        val response: ResponseEntity<List<NewsArticle>> = newsArticlesController.getAllTransformedNews()

        verify(newsArticlesRepository, times(1)).findAll()

        assertEquals(HttpStatus.NO_CONTENT, response.statusCode)
    }
}