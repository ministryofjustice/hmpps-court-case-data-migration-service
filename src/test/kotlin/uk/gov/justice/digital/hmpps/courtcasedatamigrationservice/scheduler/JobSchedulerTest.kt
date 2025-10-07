package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.MockedConstruction
import org.mockito.Mockito.mockConstruction
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import javax.sql.DataSource

class JobSchedulerTest {

  private lateinit var jobService: JobService
  private lateinit var dataSource: DataSource
  private lateinit var jobScheduler: JobScheduler
  private var jdbcTemplateConstruction: MockedConstruction<JdbcTemplate>? = null

  private val jobType = JobType.OFFENCE

  @BeforeEach
  fun setup() {
    jobService = mock()
    dataSource = mock()
  }

  @AfterEach
  fun tearDown() {
    jdbcTemplateConstruction?.close()
  }

  @Test
  fun `should run job when enabled`() {
    jdbcTemplateConstruction = mockConstruction(JdbcTemplate::class.java) { mock, _ ->
      whenever(mock.queryForObject(any(), eq(Boolean::class.java), eq(jobType.name))).thenReturn(true)
    }

    jobScheduler = JobScheduler(jobService, dataSource, jobType)
    jobScheduler.runJob()

    verify(jobService).runJob()
  }

  @Test
  fun `should not run job when disabled`() {
    jdbcTemplateConstruction = mockConstruction(JdbcTemplate::class.java) { mock, _ ->
      whenever(mock.queryForObject(any(), eq(Boolean::class.java), eq(jobType.name))).thenReturn(false)
    }

    jobScheduler = JobScheduler(jobService, dataSource, jobType)
    jobScheduler.runJob()

    verify(jobService, never()).runJob()
  }

  @Test
  fun `should handle exception gracefully`() {
    jdbcTemplateConstruction = mockConstruction(JdbcTemplate::class.java) { mock, _ ->
      whenever(mock.queryForObject(any(), eq(Boolean::class.java), eq(jobType.name))).thenThrow(RuntimeException("DB error"))
    }

    jobScheduler = JobScheduler(jobService, dataSource, jobType)
    val result = jobScheduler.runJob()

    assertThat(result).isInstanceOf(ResponseEntity::class.java)
    val response = result as ResponseEntity<*>
    assertThat(response.statusCode.value()).isEqualTo(500)
    assertThat(response.body.toString()).contains("Job failed to start")
    verify(jobService, never()).runJob()
  }
}
