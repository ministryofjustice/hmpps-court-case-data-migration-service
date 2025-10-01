package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.OffenceJobService

@RestController
class JobController(
  private val offenceJobService: OffenceJobService,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(JobController::class.java)
  }

  @GetMapping("/run-job")
  fun runJob(): ResponseEntity<String> = try {
    log.info("Received HTTP Request to start Offence Job")
    offenceJobService.runJob()
  } catch (ex: Exception) {
    ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
  }
}
