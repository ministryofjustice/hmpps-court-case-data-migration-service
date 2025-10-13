package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import nl.altindag.log.LogCaptor
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.jdbc.core.JdbcTemplate

class RowCountListenerTest {

  private lateinit var sourceJdbcTemplate: JdbcTemplate
  private lateinit var targetJdbcTemplate: JdbcTemplate
  private lateinit var listener: RowCountListener
  private lateinit var jobExecution: JobExecution
  private lateinit var logCaptor: LogCaptor

  private val sourceQuery = "SELECT COUNT(*) FROM source_table"
  private val targetQuery = "SELECT COUNT(*) FROM target_table"

  @BeforeEach
  fun setup() {
    sourceJdbcTemplate = mock()
    targetJdbcTemplate = mock()
    listener = RowCountListener(sourceJdbcTemplate, targetJdbcTemplate, sourceQuery, targetQuery)
    jobExecution = JobExecution(1L)
    logCaptor = LogCaptor.forClass(RowCountListener::class.java)
    logCaptor.clearLogs()
  }

  @Test
  fun `should log success when row counts match`() {
    whenever(sourceJdbcTemplate.queryForObject(sourceQuery, Int::class.java)).thenReturn(100)
    whenever(targetJdbcTemplate.queryForObject(targetQuery, Int::class.java)).thenReturn(100)

    jobExecution.status = BatchStatus.COMPLETED
    listener.afterJob(jobExecution)

    assertThat(logCaptor.infoLogs).contains(
      "Source row count: 100",
      "Target row count: 100",
      "Job completed successfully.",
    )
  }

  @Test
  fun `should log warning when row counts do not match`() {
    whenever(sourceJdbcTemplate.queryForObject(sourceQuery, Int::class.java)).thenReturn(100)
    whenever(targetJdbcTemplate.queryForObject(targetQuery, Int::class.java)).thenReturn(90)

    jobExecution.status = BatchStatus.COMPLETED
    listener.afterJob(jobExecution)

    assertThat(logCaptor.infoLogs).contains(
      "Source row count: 100",
      "Target row count: 90",
      "Job completed successfully.",
    )
  }

  @Test
  fun `should log failure when job is unsuccessful`() {
    whenever(sourceJdbcTemplate.queryForObject(sourceQuery, Int::class.java)).thenReturn(100)
    whenever(targetJdbcTemplate.queryForObject(targetQuery, Int::class.java)).thenReturn(100)

    jobExecution.status = BatchStatus.FAILED
    listener.afterJob(jobExecution)

    assertThat(logCaptor.infoLogs).contains(
      "Source row count: 100",
      "Target row count: 100",
      "Job failed with status: FAILED",
    )
  }

  @Test
  fun `should log success and row counts match when row counts match`() {
    whenever(sourceJdbcTemplate.queryForObject(sourceQuery, Int::class.java)).thenReturn(100)
    whenever(targetJdbcTemplate.queryForObject(targetQuery, Int::class.java)).thenReturn(100)

    val jobParameters = JobParametersBuilder()
      .addLong("batchSize", 15)
      .addLong("currentBatchCount", 14)
      .toJobParameters()
    val jobExecution = JobExecution(1L, jobParameters)
    jobExecution.status = BatchStatus.COMPLETED
    listener.afterJob(jobExecution)

    assertThat(logCaptor.infoLogs).contains(
      "✅ Row counts match. Migration looks successful.",
    )
  }

  @Test
  fun `should log warning and row counts do not match when row counts do not match`() {
    whenever(sourceJdbcTemplate.queryForObject(sourceQuery, Int::class.java)).thenReturn(100)
    whenever(targetJdbcTemplate.queryForObject(targetQuery, Int::class.java)).thenReturn(90)

    val jobParameters = JobParametersBuilder()
      .addLong("batchSize", 15)
      .addLong("currentBatchCount", 14)
      .toJobParameters()
    val jobExecution = JobExecution(1L, jobParameters)
    jobExecution.status = BatchStatus.COMPLETED
    listener.afterJob(jobExecution)

    assertThat(logCaptor.infoLogs).contains(
      "⚠️ Row counts do not match. Please investigate.",
    )
  }
}
