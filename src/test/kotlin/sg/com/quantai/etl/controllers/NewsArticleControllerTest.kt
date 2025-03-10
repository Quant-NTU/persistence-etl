package sg.com.quantai.etl.controllers

import sg.com.quantai.etl.data.NewsArticle
import sg.com.quantai.etl.data.NewsArticleBBC
import sg.com.quantai.etl.exceptions.NewsArticleException
import sg.com.quantai.etl.repositories.NewsArticleRepository
import sg.com.quantai.etl.repositories.NewsArticleBBCRepository
import sg.com.quantai.etl.services.newsArticle.NewsArticleBBCService
import sg.com.quantai.etl.services.newsArticle.NewsArticleService

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

import org.junit.jupiter.api.AfterEach
import org.mockito.ArgumentMatchers.anyList
import sg.com.quantai.etl.schedulers.NewsArticleScheduler
import sg.com.quantai.etl.schedulers.NewsArticleTransformationScheduler


@ExtendWith(SpringExtension::class)
class NewsArticleControllerTest {
    @Mock private lateinit var newsArticleService: NewsArticleService
    @Mock private lateinit var newsArticleBBCService: NewsArticleBBCService
    @Mock private lateinit var newsArticleRepository: NewsArticleRepository
    @Mock private lateinit var newsArticleBBCRepository: NewsArticleBBCRepository
    @InjectMocks private lateinit var newsArticleScheduler: NewsArticleScheduler
    @InjectMocks private lateinit var newsArticleTransformationScheduler: NewsArticleTransformationScheduler
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

        `when`(newsArticleBBCRepository.existsByLink("http://example.com/article1")).thenReturn(false)
        `when`(newsArticleBBCRepository.existsByLink("http://example.com/article2")).thenReturn(false)

        `when`(newsArticleBBCRepository.countByLink("http://example.com/article1")).thenReturn(0)
        `when`(newsArticleBBCRepository.countByLink("http://example.com/article2")).thenReturn(0)

        `when`(newsArticleBBCRepository.save(newsArticleBBC1)).thenReturn(newsArticleBBC1)
        `when`(newsArticleBBCRepository.save(newsArticleBBC2)).thenReturn(newsArticleBBC2)

        `when`(newsArticleController.fetchAndSaveBBCNews()).thenAnswer {
            mockArticles.forEach { article ->
                if (!newsArticleBBCRepository.existsByLink(article.link)) {
                    newsArticleBBCRepository.save(article)
                    `when`(newsArticleBBCRepository.existsByLink(article.link)).thenReturn(true)
                    `when`(newsArticleBBCRepository.countByLink(article.link)).thenReturn(1)
                }
            }
            ResponseEntity.ok("News articles fetched and saved.")
        }
        val response = newsArticleController.fetchAndSaveBBCNews()

        verify(newsArticleBBCRepository, times(1)).save(newsArticleBBC1)
        verify(newsArticleBBCRepository, times(1)).save(newsArticleBBC2)
        verify(newsArticleBBCRepository, times(0)).save(newsArticleBBC3)

        assertEquals(HttpStatus.OK, response.statusCode)
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

    @Test
    fun `should transform only untransformed raw articles`() {
        val untransformedRawArticle = NewsArticleBBC(
            title = "Untransformed Title",
            publishedDate = LocalDate.parse("2023-10-01"),
            description = "Untransformed Description",
            content = "Untransformed Content",
            link = "http://example.com/untransformed",
            topImage = "image1",
            transformed = false
        )
        val transformedRawArticle = NewsArticleBBC(
            title = "Transformed Title",
            publishedDate = LocalDate.parse("2023-10-02"),
            description = "Transformed Description",
            content = "Transformed Content",
            link = "http://example.com/transformed",
            topImage = "image2",
            transformed = true
        )

        `when`(newsArticleBBCRepository.findAllByTransformedFalse()).thenReturn(listOf(untransformedRawArticle))

        val newsArticleService = NewsArticleService(newsArticleBBCRepository, newsArticleRepository)

        newsArticleService.transformAndSave()

        verify(newsArticleBBCRepository, times(1)).findAllByTransformedFalse()

        verify(newsArticleRepository, times(1)).saveAll(anyList())

        verify(newsArticleRepository, times(0)).saveAll(
            listOf(
                NewsArticle(
                    title = transformedRawArticle.title,
                    publishedDate = transformedRawArticle.publishedDate,
                    description = transformedRawArticle.description ?: "",
                    content = transformedRawArticle.content
                )
            )
        )
    }

    @Test
    fun `should run scheduled fetch and transform news`() {
        doNothing().`when`(newsArticleBBCService).fetchAndSaveAllMonths()
        doNothing().`when`(newsArticleService).transformAndSave()

        newsArticleScheduler.pull()
        verify(newsArticleBBCService, times(1)).fetchAndSaveAllMonths()

        newsArticleTransformationScheduler.transform()
        verify(newsArticleService, times(1)).transformAndSave()
    }
}