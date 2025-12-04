package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.job

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

@Configuration
class SyncJobConfig(
  @Qualifier("syncOffenderIdInDefendantJobService")
  private val syncOffenderIdInDefendantJobService: JobService,
  @Qualifier("syncDefendantIdInCaseCommentJobService")
  private val syncDefendantIdInCaseCommentJobService: JobService,
  @Qualifier("syncCaseIdInCaseCommentJobService")
  private val syncCaseIdInCaseCommentJobService: JobService,
  @Qualifier("syncOffenceIdInDefendantOffenceJobService")
  private val syncOffenceIdInDefendantOffenceJobService: JobService,
  @Qualifier("syncDefendantIdInDefendantOffenceJobService")
  private val syncDefendantIdInDefendantOffenceJobService: JobService,
  @Qualifier("syncProsecutionCaseIdInOffenderMatchGroupJobService")
  private val syncProsecutionCaseIdInOffenderMatchGroupJobService: JobService,
  @Qualifier("syncDefendantIdInOffenderMatchGroupJobService")
  private val syncDefendantIdInOffenderMatchGroupJobService: JobService,
  @Qualifier("syncOffenderMatchGroupIdInOffenderMatchJobService")
  private val syncOffenderMatchGroupIdInOffenderMatchJobService: JobService,
  @Qualifier("syncOffenderIdInOffenderMatchJobService")
  private val syncOffenderIdInOffenderMatchJobService: JobService,
) {

  @Bean
  fun syncJobMap(): Map<SyncJobType, () -> Unit> = linkedMapOf(
    SyncJobType.OFFENDER_ID_DEFENDANT to { syncOffenderIdInDefendantJobService.runJob() },
    SyncJobType.DEFENDANT_ID_CASE_COMMENT to { syncDefendantIdInCaseCommentJobService.runJob() },
    SyncJobType.CASE_ID_CASE_COMMENT to { syncCaseIdInCaseCommentJobService.runJob() },
    SyncJobType.OFFENCE_ID_DEFENDANT_OFFENCE to { syncOffenceIdInDefendantOffenceJobService.runJob() },
    SyncJobType.DEFENDANT_ID_DEFENDANT_OFFENCE to { syncDefendantIdInDefendantOffenceJobService.runJob() },
    SyncJobType.PROSECUTION_CASE_ID_OFFENDER_MATCH_GROUP to { syncProsecutionCaseIdInOffenderMatchGroupJobService.runJob() },
    SyncJobType.DEFENDANT_ID_OFFENDER_MATCH_GROUP to { syncDefendantIdInOffenderMatchGroupJobService.runJob() },
    SyncJobType.OFFENDER_MATCH_GROUP_ID_OFFENDER_MATCH to { syncOffenderMatchGroupIdInOffenderMatchJobService.runJob() },
    SyncJobType.OFFENDER_ID_OFFENDER_MATCH to { syncOffenderIdInOffenderMatchJobService.runJob() },
  )
}
