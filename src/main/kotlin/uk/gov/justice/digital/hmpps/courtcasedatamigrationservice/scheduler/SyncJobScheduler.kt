package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.springframework.scheduling.annotation.Scheduled
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

class SyncJobScheduler(
  jobService: JobService,
  schedulingConfigRepository: SchedulingConfigRepository,
  private val syncJobType: SyncJobType,
) : Scheduler(jobService, schedulingConfigRepository) {

  @Scheduled(cron = "0 0 * * * *")
  fun runJob() {
    super.runJob(syncJobType.name)
  }
}
