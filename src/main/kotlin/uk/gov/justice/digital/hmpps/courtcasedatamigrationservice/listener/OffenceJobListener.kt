package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class OffenceJobListener(
  @Qualifier("sourceJdbcTemplate") private val sourceJdbcTemplate: JdbcTemplate,
  @Qualifier("targetJdbcTemplate") private val targetJdbcTemplate: JdbcTemplate,
) : JobExecutionListener {

  private val log = LoggerFactory.getLogger(OffenceJobListener::class.java)

  override fun afterJob(jobExecution: JobExecution) {
    val sourceCount = sourceJdbcTemplate.queryForObject("SELECT COUNT(*) FROM courtcaseservice.offence", Int::class.java)
    val targetCount = targetJdbcTemplate.queryForObject("SELECT COUNT(*) FROM hmpps_court_case_service.offence", Int::class.java)

    log.info("Source row count: $sourceCount")
    log.info("Target row count: $targetCount")

    if (jobExecution.status.isUnsuccessful) {
      log.info("Job failed with status: ${jobExecution.status}")
    } else {
      log.info("Job completed successfully.")
      if (sourceCount == targetCount) {
        log.info("✅ Row counts match. Migration looks successful.")
      } else {
        log.info("⚠️ Row counts do not match. Please investigate.")
      }
    }
  }
}
