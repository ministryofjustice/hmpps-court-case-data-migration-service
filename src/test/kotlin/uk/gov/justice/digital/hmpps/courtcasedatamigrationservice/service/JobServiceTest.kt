package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate

class JobServiceTest {

  private lateinit var jobLauncher: JobLauncher
  private lateinit var job: Job
  private lateinit var jdbcTemplate: JdbcTemplate
  private lateinit var jobService: JobService

  private val batchSize = 3
  private val minQuery = "SELECT MIN(id) FROM cases"
  private val maxQuery = "SELECT MAX(id) FROM cases"
  private val jobName = "TestJob"

  @BeforeEach
  fun setup() {
    jobLauncher = mock()
    job = mock()
    jdbcTemplate = mock()

    jobService = JobService(
      jobLauncher = jobLauncher,
      job = job,
      jdbcTemplate = jdbcTemplate,
      batchSize = batchSize,
      minQuery = minQuery,
      maxQuery = maxQuery,
      jobName = jobName,
    )
  }

  @Test
  @Disabled
  fun `should run job with correct chunking`() {
    whenever(jdbcTemplate.queryForObject(minQuery, Long::class.java)).thenReturn(1L)
    whenever(jdbcTemplate.queryForObject(maxQuery, Long::class.java)).thenReturn(10L)

    val response = jobService.runJob()

    assertThat(ResponseEntity.ok("$jobName job completed. Check logs for details.")).isEqualTo(response)
    verify(jobLauncher, times(batchSize)).run(eq(job), any<JobParameters>())
  }

  @Test
  fun `should return error if minId is null`() {
    whenever(jdbcTemplate.queryForObject(minQuery, Long::class.java)).thenReturn(null)
    whenever(jdbcTemplate.queryForObject(maxQuery, Long::class.java)).thenReturn(10L)

    val response = jobService.runJob()

    assertThat(response.statusCode.value()).isEqualTo(500)
    assertThat(response.body).isEqualTo("$jobName job failed: MinId and MaxId must be provided and non-null")
    verify(jobLauncher, never()).run(any(), any())
  }

  @Test
  fun `should return error if batchSize is zero`() {
    jobService = JobService(jobLauncher, job, jdbcTemplate, 0, minQuery, maxQuery, jobName)

    whenever(jdbcTemplate.queryForObject(minQuery, Long::class.java)).thenReturn(1L)
    whenever(jdbcTemplate.queryForObject(maxQuery, Long::class.java)).thenReturn(10L)

    val response = jobService.runJob()

    assertThat(response.statusCode.value()).isEqualTo(500)
    assertThat(response.body).isEqualTo("$jobName job failed: Batch size must be greater than 0")
    verify(jobLauncher, never()).run(any(), any())
  }

  @Test
  fun `should return error if ID range is invalid`() {
    whenever(jdbcTemplate.queryForObject(minQuery, Long::class.java)).thenReturn(10L)
    whenever(jdbcTemplate.queryForObject(maxQuery, Long::class.java)).thenReturn(5L)

    val response = jobService.runJob()

    assertThat(response.statusCode.value()).isEqualTo(500)
    assertThat(response.body).isEqualTo("$jobName job failed: Invalid ID range: minId=10, maxId=5")
    verify(jobLauncher, never()).run(any(), any())
  }
}
