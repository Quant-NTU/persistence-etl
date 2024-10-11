package sg.com.quantai.etl.controllers

import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.exceptions.NewsArticleException
import sg.com.quantai.etl.repositories.NewsArticleRepository
import sg.com.quantai.etl.services.NewsArticleBBCService
import sg.com.quantai.etl.services.NewsArticleService

import java.time.LocalDate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.test.context.junit.jupiter.SpringExtension

@ExtendWith(SpringExtension::class)
class NewsArticleControllerTest {
    @Mock private lateinit var newsArticleService: NewsArticleService
    @Mock private lateinit var newsArticleBBCService: NewsArticleBBCService
    @Mock private lateinit var newsArticleRepository: NewsArticleRepository
    @InjectMocks private lateinit var newsArticleController: NewsArticleController

    @Test
    fun `should connect to BBC news Hugging Face API and fetch results`() {
        val response: ResponseEntity<String> = newsArticleController.fetchAndSaveBBCNews()

        verify(newsArticleBBCService, times(1)).fetchAndSaveAllMonths()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("News articles fetched and saved.", response.body)
    }

    @Test
    fun `should apply transformations on BBC news data and save results`() {
        doNothing().`when`(newsArticleService).transformAndSave()

        val response: ResponseEntity<String> = newsArticleController.transformBBC()

        verify(newsArticleService, times(1)).transformAndSave()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Transformation completed successfully and results have been stored in database.", response.body)
    }

    @Test
    fun `should yield proper exception if a transformation fails`() {
        doThrow(NewsArticleException("Transformation failed")).`when`(newsArticleService).transformAndSave()

        val response: ResponseEntity<String> = newsArticleController.transformBBC()

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("Transformation failed: Transformation failed", response.body)
    }

    @Test
    fun `should yield transformed articles if at least one is available`() {
        val mockArticles = listOf(
            NewsArticle(
                title = "Title 1",
                publishedDate = LocalDate.parse("2023-10-01"),
                description = "Description 1",
                content = "Content 1"),
            NewsArticle(
                title = "Title 2",
                publishedDate = LocalDate.parse("2023-10-02"),
                description = "Description 2",
                content = "Content 2")
        )
        `when`(newsArticleRepository.findAll()).thenReturn(mockArticles)

        val response: ResponseEntity<List<NewsArticle>> = newsArticleController.findAll()

        verify(newsArticleRepository, times(1)).findAll()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(mockArticles, response.body)
    }

    @Test
    fun `should return a empty response if no articles are available`() {
        `when`(newsArticleRepository.findAll()).thenReturn(emptyList())

        val response: ResponseEntity<List<NewsArticle>> = newsArticleController.findAll()

        verify(newsArticleRepository, times(1)).findAll()

        assertEquals(HttpStatus.NO_CONTENT, response.statusCode)
    }
}