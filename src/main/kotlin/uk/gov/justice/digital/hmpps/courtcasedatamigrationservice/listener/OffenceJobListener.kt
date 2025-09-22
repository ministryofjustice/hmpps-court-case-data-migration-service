package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class OffenceJobListener(
  @Qualifier("sourceJdbcTemplate") private val sourceJdbcTemplate: JdbcTemplate,
  @Qualifier("targetJdbcTemplate") private val targetJdbcTemplate: JdbcTemplate,
) : JobExecutionListener {

  private val log = LoggerFactory.getLogger(OffenceJobListener::class.java)
  private var startTime: Instant? = null

  override fun beforeJob(jobExecution: JobExecution) {
    log.info("Job is starting: ${jobExecution.jobInstance.jobName}")
    startTime = Instant.now()
    log.info("Job started at: $startTime")
  }

  override fun afterJob(jobExecution: JobExecution) {
    val endTime = Instant.now()
    log.info("Job ended at: $endTime")

    val duration = Duration.between(startTime, endTime)
    log.info("Total job duration: ${duration.toMinutes()} minutes and ${duration.seconds % 60} seconds")

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
