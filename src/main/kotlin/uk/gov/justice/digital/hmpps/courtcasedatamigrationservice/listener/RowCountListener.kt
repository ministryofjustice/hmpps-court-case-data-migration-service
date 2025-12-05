package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import java.text.NumberFormat
import java.util.Locale

class RowCountListener(
  @Qualifier("sourceJdbcTemplate") private val sourceJdbcTemplate: JdbcTemplate,
  @Qualifier("targetJdbcTemplate") private val targetJdbcTemplate: JdbcTemplate,
  private val sourceRowCountQuery: String,
  private val targetRowCountQuery: String,
) : JobExecutionListener {

  private val log = LoggerFactory.getLogger(RowCountListener::class.java)

  override fun afterJob(jobExecution: JobExecution) {
    val sourceCount = sourceJdbcTemplate.queryForObject(this.sourceRowCountQuery, Int::class.java)
    val targetCount = targetJdbcTemplate.queryForObject(this.targetRowCountQuery, Int::class.java)

    val numberInstance = NumberFormat.getNumberInstance(Locale.UK)
    log.info("Source row count: ${numberInstance.format(sourceCount)}")
    log.info("Target row count: ${numberInstance.format(targetCount)}")

    if (jobExecution.status.isUnsuccessful) {
      log.info("Job failed with status: ${jobExecution.status}")
    } else {
      log.info("Job completed successfully.")
    }

    if (isLastJobRun(jobExecution)) {
      if (sourceCount == targetCount) {
        log.info("✅ Row counts match. Migration looks successful.")
      } else {
        log.info("⚠️ Row counts do not match. Please investigate.")
      }
    }
  }

  private fun isLastJobRun(jobExecution: JobExecution): Boolean {
    val jobParameters = jobExecution.jobParameters
    val currentBatchCount = jobParameters.getLong("currentBatchCount")
    val batchSize = jobParameters.getLong("batchSize")
    return currentBatchCount != null && batchSize != null && currentBatchCount + 1 == batchSize
  }
}
