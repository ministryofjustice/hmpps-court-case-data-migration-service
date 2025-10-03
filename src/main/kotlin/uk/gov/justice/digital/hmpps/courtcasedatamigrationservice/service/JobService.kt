package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant

class JobService(
  private val jobLauncher: JobLauncher,
  private val job: Job,
  private val sourceJdbcTemplate: JdbcTemplate,
  private val batchSize: Int,
  private val minQuery: String,
  private val maxQuery: String,
  private val jobName: String,
) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(JobService::class.java)
  }

  private var startTime: Instant? = null

  fun runJob(): ResponseEntity<String> = try {
    beforeJob()

    val minId = sourceJdbcTemplate.queryForObject(this.minQuery, Int::class.java)
    val maxId = sourceJdbcTemplate.queryForObject(this.maxQuery, Int::class.java)

    log.info("Min ID: $minId")
    log.info("Max ID: $maxId")

    if (minId == null || maxId == null) {
      throw IllegalArgumentException("MinId and MaxId must be provided")
    }

    val total = maxId - minId + 1
    val chunkSize = total / batchSize

    for (i in 0 until batchSize) {
      val chunkMin = minId + (i * chunkSize)
      val chunkMax = if (i == batchSize - 1) maxId else chunkMin + chunkSize - 1

      val params = JobParametersBuilder()
        .addString("run.id", "${Instant.now()}-$i")
        .addLong("minId", chunkMin.toLong())
        .addLong("maxId", chunkMax.toLong())
        .toJobParameters()

      jobLauncher.run(job, params)
    }

    afterJob()
    ResponseEntity.ok("$jobName Job Completed. Check logs.")
  } catch (ex: Exception) {
    ResponseEntity.internalServerError().body("$jobName Job failed to start: ${ex.message}")
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
