package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.slf4j.LoggerFactory
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate

class SchedulingConfigRepository(private val jdbcTemplate: JdbcTemplate) {

  private companion object {
    private val log = LoggerFactory.getLogger(SchedulingConfigRepository::class.java)
  }

  fun isJobEnabled(jobName: String): Boolean = try {
    jdbcTemplate.queryForObject(
      """
          SELECT IS_ENABLED 
          FROM hmpps_court_case_batch_metadata.HMPPS_BATCH_SCHEDULING_CONFIG 
          WHERE JOB_NAME = ?
      """.trimIndent(),
      Boolean::class.java,
      jobName,
    ) ?: false
  } catch (e: EmptyResultDataAccessException) {
    log.warn("No scheduling configuration found for job '{}'", jobName)
    false
  }

  fun countEnabledJobs(): Int = jdbcTemplate.queryForObject(
    """
        SELECT COUNT(*) 
        FROM hmpps_court_case_batch_metadata.HMPPS_BATCH_SCHEDULING_CONFIG 
        WHERE IS_ENABLED = true
    """.trimIndent(),
    Int::class.java,
  ) ?: 0
}
