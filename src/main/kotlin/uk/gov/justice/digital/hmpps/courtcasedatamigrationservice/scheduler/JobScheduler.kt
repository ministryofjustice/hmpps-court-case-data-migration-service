package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import javax.sql.DataSource

class JobScheduler(
  private val jobService: JobService,
  private val dataSource: DataSource,
  private val jobType: JobType,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(JobScheduler::class.java)
  }

  @Scheduled(cron = "0 0 * * * *")
  fun runJob() = try {
    log.info("Received Scheduler Request to start $jobType Job")
    val jdbcTemplate = JdbcTemplate(dataSource)
    val isEnabled = jdbcTemplate.queryForObject(
      "SELECT IS_ENABLED FROM hmpps_court_case_batch_metadata.HMPPS_BATCH_SCHEDULING_CONFIG WHERE JOB_NAME = ?",
      Boolean::class.java,
      jobType.name,
    )
    if (isEnabled != null && isEnabled) {
      log.info("$jobType job enabled: $isEnabled. Starting scheduled Job.")
      jobService.runJob()
    } else {
      log.info("$jobType job disabled: $isEnabled. Ending process.")
    }
  } catch (ex: Exception) {
    ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
  }
}
