package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

class JobScheduler(
  private val jobService: JobService,
  private val schedulingConfigRepository: SchedulingConfigRepository,
  private val jobType: JobType,
) {

  private companion object {
    private val log = LoggerFactory.getLogger(JobScheduler::class.java)
  }

  @Scheduled(cron = "0 0 * * * *")
  fun runJob() {
    try {
      log.info("Received Scheduler Request to start {} Job", jobType)

      if (isMultipleJobsEnabled()) {
        log.info("Multiple jobs are enabled. Skipping execution of {} job.", jobType)
        return
      }

      if (schedulingConfigRepository.isJobEnabled(jobType.name)) {
        log.info("{} job is enabled. Starting scheduled job.", jobType)
        jobService.runJob()
      } else {
        log.info("{} job is disabled. Ending process.", jobType)
      }
    } catch (ex: Exception) {
      log.error("Job {} failed to start: {}", jobType, ex.message, ex)
    }
  }

  private fun isMultipleJobsEnabled(): Boolean {
    val jobEnabledCount = schedulingConfigRepository.countEnabledJobs()
    return if (jobEnabledCount > 1) {
      log.warn("Found {} jobs enabled. Only one job should be enabled at a time.", jobEnabledCount)
      true
    } else {
      false
    }
  }
}
