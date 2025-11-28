package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.job

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

@Configuration
class JobConfig(
  @Qualifier("offenceJobService")
  private val offenceJobService: JobService,
  @Qualifier("defendantJobService")
  private val defendantJobService: JobService,
  @Qualifier("hearingJobService")
  private val hearingJobService: JobService,
  @Qualifier("defendantOffenceJobService")
  private val defendantOffenceJobService: JobService,
  @Qualifier("caseJobService")
  private val caseJobService: JobService,
  @Qualifier("caseCommentJobService")
  private val caseCommentJobService: JobService,
  @Qualifier("courtJobService")
  private val courtJobService: JobService,
  @Qualifier("offenderJobService")
  private val offenderJobService: JobService,
  @Qualifier("offenderMatchGroupJobService")
  private val offenderMatchGroupJobService: JobService,
  @Qualifier("offenderMatchJobService")
  private val offenderMatchJobService: JobService,
) {

  @Bean
  fun jobMap(): Map<JobType, () -> Unit> = linkedMapOf(
    JobType.OFFENCE to { offenceJobService.runJob() },
    JobType.OFFENDER to { offenderJobService.runJob() },
    JobType.DEFENDANT to { defendantJobService.runJob() },
    JobType.HEARING to { hearingJobService.runJob() },
    JobType.CASE to { caseJobService.runJob() },
    JobType.CASE_COMMENT to { caseCommentJobService.runJob() },
    JobType.COURT to { courtJobService.runJob() },
    JobType.OFFENDER_MATCH_GROUP to { offenderMatchGroupJobService.runJob() },
    JobType.OFFENDER_MATCH to { offenderMatchJobService.runJob() },
    JobType.DEFENDANT_OFFENCE to { defendantOffenceJobService.runJob() },
  )
}
