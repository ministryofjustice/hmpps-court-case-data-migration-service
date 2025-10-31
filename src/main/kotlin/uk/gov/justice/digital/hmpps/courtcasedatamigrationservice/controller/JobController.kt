package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

@RestController
class JobController(
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
  @Qualifier("courtJobService")
  private val courtJobService: JobService,
  @Qualifier("offenderJobService")
  private val offenderJobService: JobService,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(JobController::class.java)
  }

  @PostMapping("/jobs/{jobType}/run")
  fun runJob(@PathVariable jobType: String): ResponseEntity<String> {
    val type = enumValues<JobType>().firstOrNull { it.name.equals(jobType, ignoreCase = true) }
      ?: return ResponseEntity.badRequest().body("Unknown job type: $jobType")

    log.info("Received request to start job: $type")
    return try {
      when (type) {
        JobType.OFFENCE -> offenceJobService.runJob()
        JobType.DEFENDANT -> defendantJobService.runJob()
        JobType.HEARING -> hearingJobService.runJob()
        JobType.CASE -> caseJobService.runJob()
        JobType.COURT -> courtJobService.runJob()
        JobType.OFFENDER -> offenderJobService.runJob()
        JobType.DEFENDANT_OFFENCE -> defendantOffenceJobService.runJob()
      }
      ResponseEntity.ok("$type job started successfully")
    } catch (ex: Exception) {
      log.error("Failed to start job: $type", ex)
      ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
    }
  }
}
