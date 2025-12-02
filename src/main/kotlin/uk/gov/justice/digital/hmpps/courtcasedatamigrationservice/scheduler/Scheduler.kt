package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.slf4j.LoggerFactory
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

abstract class Scheduler(
  private val jobService: JobService,
  private val schedulingConfigRepository: SchedulingConfigRepository,
) {

  private companion object {
    private val log = LoggerFactory.getLogger(Scheduler::class.java)
  }

  open fun runJob(jobName: String) {
    try {
      log.info("Received Scheduler Request to start {} Job", jobName)

      if (isMultipleJobsEnabled()) {
        log.info("Multiple jobs are enabled. Skipping execution of {} job.", jobName)
        return
      }

      if (schedulingConfigRepository.isJobEnabled(jobName)) {
        log.info("{} job is enabled. Starting scheduled job.", jobName)
        jobService.runJob()
      } else {
        log.info("{} job is disabled. Ending process.", jobName)
      }
    } catch (ex: Exception) {
      log.error("Job {} failed to start: {}", jobName, ex.message, ex)
    }
  }

  fun isMultipleJobsEnabled(): Boolean {
    val jobEnabledCount = schedulingConfigRepository.countEnabledJobs()
    return if (jobEnabledCount > 1) {
      log.warn("Found {} jobs enabled. Only one job should be enabled at a time.", jobEnabledCount)
      true
    } else {
      false
    }
  }
}
