package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

@RestController
class SyncJobController(
  @Qualifier("syncOffenderIdInDefendantJobService")
  private val syncOffenderIdInDefendantJobService: JobService,
  @Qualifier("syncDefendantIdInDefendantOffenceJobService")
  private val syncDefendantIdInDefendantOffenceJobService: JobService,
  @Qualifier("syncOffenceIdInDefendantOffenceJobService")
  private val syncOffenceIdInDefendantOffenceJobService: JobService,
  @Qualifier("syncOffenderIdInOffenderMatchJobService")
  private val syncOffenderIdInOffenderMatchJobService: JobService,
  @Qualifier("syncOffenderMatchGroupIdInOffenderMatchJobService")
  private val syncOffenderMatchGroupIdInOffenderMatchJobService: JobService,
  @Qualifier("syncDefendantIdInOffenderMatchGroupJobService")
  private val syncDefendantIdInOffenderMatchGroupJobService: JobService,
  @Qualifier("syncProsecutionCaseIdInOffenderMatchGroupJobService")
  private val syncProsecutionCaseIdInOffenderMatchGroupJobService: JobService,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(SyncJobController::class.java)
  }

  @PostMapping("/jobs/sync/{syncJobType}/run")
  fun runJob(@PathVariable syncJobType: String): ResponseEntity<String> {
    val type = enumValues<SyncJobType>().firstOrNull { it.name.equals(syncJobType, ignoreCase = true) }
      ?: return ResponseEntity.badRequest().body("Unknown job type: $syncJobType")

    log.info("Received request to start job: $type")
    return try {
      when (type) {
        SyncJobType.OFFENDER_ID_DEFENDANT -> syncOffenderIdInDefendantJobService.runJob()
        SyncJobType.DEFENDANT_ID_DEFENDANT_OFFENCE -> syncDefendantIdInDefendantOffenceJobService.runJob()
        SyncJobType.OFFENCE_ID_DEFENDANT_OFFENCE -> syncOffenceIdInDefendantOffenceJobService.runJob()
        SyncJobType.OFFENDER_ID_OFFENDER_MATCH -> syncOffenderIdInOffenderMatchJobService.runJob()
        SyncJobType.OFFENDER_MATCH_GROUP_ID_OFFENDER_MATCH -> syncOffenderMatchGroupIdInOffenderMatchJobService.runJob()
        SyncJobType.DEFENDANT_ID_OFFENDER_MATCH_GROUP -> syncDefendantIdInOffenderMatchGroupJobService.runJob()
        SyncJobType.PROSECUTION_CASE_ID_OFFENDER_MATCH_GROUP -> syncProsecutionCaseIdInOffenderMatchGroupJobService.runJob()
      }
      ResponseEntity.ok("$type job started successfully")
    } catch (ex: Exception) {
      log.error("Failed to start job: $type", ex)
      ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
    }
  }
}
