package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class TimerJobListener : JobExecutionListener {

  private val log = LoggerFactory.getLogger(TimerJobListener::class.java)
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
    log.info("Job duration: ${duration.toMinutes()} minutes and ${duration.seconds % 60} seconds")
  }
}
