package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType

@RestController
class JobController(
  private val jobMap: Map<JobType, () -> Unit>,
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
      jobMap[type]?.invoke()
      ResponseEntity.ok("$type job started successfully")
    } catch (ex: Exception) {
      log.error("Failed to start job: $type", ex)
      ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
    }
  }
}
