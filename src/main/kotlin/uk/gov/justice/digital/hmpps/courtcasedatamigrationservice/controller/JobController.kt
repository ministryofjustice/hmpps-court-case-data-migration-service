package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.time.Instant

@RestController
class JobController(
  private val jobLauncher: JobLauncher,
  private val job: Job,
  @Qualifier("sourceJdbcTemplate") private val sourceJdbcTemplate: JdbcTemplate,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(JobController::class.java)
  }

  private var startTime: Instant? = null

  @GetMapping("/run-job")
  fun runJob(): ResponseEntity<String> = try {
    beforeJob()

    val minId = sourceJdbcTemplate.queryForObject("SELECT MIN(id) FROM courtcaseservice.offence", Int::class.java)
    val maxId = sourceJdbcTemplate.queryForObject("SELECT MAX(id) FROM courtcaseservice.offence", Int::class.java)

    log.info("Min ID: $minId")
    log.info("Max ID: $maxId")

    val total = maxId - minId + 1
    val chunkSize = total / 10

    for (i in 0 until 10) {
      val chunkMin = minId + (i * chunkSize)
      val chunkMax = if (i == 9) maxId else chunkMin + chunkSize - 1

      val params = JobParametersBuilder()
        .addString("run.id", "${Instant.now()}-$i")
        .addLong("minId", chunkMin.toLong())
        .addLong("maxId", chunkMax.toLong())
        .toJobParameters()

      jobLauncher.run(job, params)
    }

    afterJob()
    ResponseEntity.ok("Offence Job Completed. Check logs.")
  } catch (ex: Exception) {
    ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
  }

  fun beforeJob() {
    log.info("Starting job")
    startTime = Instant.now()
    log.info("Job started at: $startTime")
  }

  fun afterJob() {
    val endTime = Instant.now()
    log.info("Job ended at: $endTime")
    val duration = Duration.between(startTime, endTime)
    log.info("Total job duration: ${duration.toMinutes()} minutes and ${duration.seconds % 60} seconds")
  }
}
