package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate

class DefendantOffenceListener(@Qualifier("targetJdbcTemplate") private val targetJdbcTemplate: JdbcTemplate) : JobExecutionListener {

  private val log = LoggerFactory.getLogger(DefendantOffenceListener::class.java)

  override fun beforeJob(jobExecution: JobExecution) {
    val currentBatchCount = jobExecution.jobParameters.getLong("currentBatchCount")
    if (currentBatchCount == 0L) {
      val sequenceName = "hmpps_court_case_service.defendant_offence_id_seq"
      log.info("Resetting sequence: $sequenceName")
      targetJdbcTemplate.execute("ALTER SEQUENCE $sequenceName RESTART WITH 1")
      log.info("Sequence reset: $sequenceName")
    }
  }
}
