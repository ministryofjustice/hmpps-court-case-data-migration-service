package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.OffenceJobService
import javax.sql.DataSource

@Component
class OffenceJobScheduler(
  private val offenceJobService: OffenceJobService,
  private val dataSource: DataSource,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(OffenceJobScheduler::class.java)
  }

  @Scheduled(cron = "0 0 * * * *")
  fun runJob() = try {
    log.info("Received Scheduler Request to start Offence Job")
    val jdbcTemplate = JdbcTemplate(dataSource)
    val isEnabled = jdbcTemplate.queryForObject(
      "SELECT IS_ENABLED FROM hmpps_court_case_batch_metadata.HMPPS_BATCH_SCHEDULING_CONFIG WHERE JOB_NAME = ?",
      Boolean::class.java,
      "OFFENCE",
    )
    if (isEnabled) {
      log.info("Offence job enabled: $isEnabled. Starting scheduled Job.")
      offenceJobService.runJob()
    } else {
      log.info("Offence job disabled: $isEnabled. Ending process.")
    }
  } catch (ex: Exception) {
    ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
  }
}
