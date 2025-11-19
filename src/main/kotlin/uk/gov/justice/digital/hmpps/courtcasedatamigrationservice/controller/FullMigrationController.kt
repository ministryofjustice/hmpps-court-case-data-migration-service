package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType
import java.time.Duration
import java.time.Instant

@RestController
class FullMigrationController(
  private val jobMap: Map<JobType, () -> Unit>,
  private val syncJobMap: Map<SyncJobType, () -> Unit>,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(FullMigrationController::class.java)
  }

  private var startTime: Instant? = null

  @PostMapping("/jobs/run-all")
  fun runAllJobs(): ResponseEntity<String> {
    log.info("Full migration is starting.")
    startTime = Instant.now()
    log.info("Full migration started at: $startTime")

    val results = StringBuilder()

    fun <T> runJobs(map: Map<T, () -> Unit>) {
      map.forEach { (jobType, jobAction) ->
        try {
          jobAction.invoke()
          results.append("$jobType job completed successfully.\n")
          log.info("Running full migration. Sleeping 10 seconds")
          Thread.sleep(10_000)
        } catch (ex: Exception) {
          results.append("$jobType job failed: ${ex.message}\n")
        }
      }
    }

    runJobs(jobMap)
    runJobs(syncJobMap)

    val endTime = Instant.now()
    log.info("Full migration ended at: $endTime")
    val duration = Duration.between(startTime, endTime)
    log.info("Full migration duration: ${duration.toMinutes()} minutes and ${duration.seconds % 60} seconds")

    return ResponseEntity.ok(results.toString())
  }
}
