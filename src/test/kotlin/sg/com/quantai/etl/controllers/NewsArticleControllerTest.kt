package sg.com.quantai.etl.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.data.NewsArticleBBC
import sg.com.quantai.etl.exceptions.NewsArticleException
import sg.com.quantai.etl.repositories.NewsArticleRepository
import sg.com.quantai.etl.repositories.NewsArticleBBCRepository
import sg.com.quantai.etl.services.NewsArticleBBCService
import sg.com.quantai.etl.services.NewsArticleService

import java.time.LocalDate
import java.time.YearMonth

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

import org.junit.jupiter.api.AfterEach


@ExtendWith(SpringExtension::class)
class NewsArticleControllerTest {
    @Mock private lateinit var newsArticleService: NewsArticleService
    @Mock private lateinit var newsArticleBBCService: NewsArticleBBCService
    @Mock private lateinit var newsArticleRepository: NewsArticleRepository
    @Mock private lateinit var newsArticleBBCRepository: NewsArticleBBCRepository
    @InjectMocks private lateinit var newsArticleController: NewsArticleController

    @AfterEach
    fun cleanUp() {
        newsArticleBBCRepository.deleteAll()
    }

    @Test
    fun `should connect to BBC news Hugging Face API and fetch results`() {
        val response: ResponseEntity<String> = newsArticleController.fetchAndSaveBBCNews()

        verify(newsArticleBBCService, times(1)).fetchAndSaveAllMonths()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("News articles fetched and saved.", response.body)
    }

    @Test
    fun `should avoid saving duplicated BBC articles`() {
        val newsArticleBBC1 = NewsArticleBBC(
            title = "Title 1",
            publishedDate = LocalDate.parse("2023-10-01"),
            description = "Description 1",
            content = "Content 1",
            link = "http://example.com/article1",
            topImage = "image1"
        )
        val newsArticleBBC2 = NewsArticleBBC(
            title = "Title 2",
            publishedDate = LocalDate.parse("2023-10-02"),
            description = "Description 2",
            content = "Content 2",
            link = "http://example.com/article2",
            topImage = "image2"
        )
        val newsArticleBBC3 = NewsArticleBBC(
            title = "Title 3",
            publishedDate = LocalDate.parse("2023-10-03"),
            description = "Description 3",
            content = "Content 3",
            link = "http://example.com/article1",
            topImage = "image1"
        )
        val mockArticles = listOf(newsArticleBBC1, newsArticleBBC2, newsArticleBBC3)

        // when then

        newsArticleController.findAll()
        verify(newsArticleController, times(1)).fetchAndSaveBBCNews()

        assertEquals(1, newsArticleBBCRepository.countByLink("http://example.com/article1"))
        assertEquals(1, newsArticleBBCRepository.countByLink("http://example.com/article2"))
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