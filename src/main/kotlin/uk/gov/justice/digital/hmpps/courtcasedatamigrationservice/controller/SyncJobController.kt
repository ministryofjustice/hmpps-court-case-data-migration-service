package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType

@RestController
class SyncJobController(
  private val syncJobMap: Map<SyncJobType, () -> Unit>,
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
      syncJobMap[type]?.invoke()
      ResponseEntity.ok("$type job started successfully")
    } catch (ex: Exception) {
      log.error("Failed to start job: $type", ex)
      ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
    }
  }
}
