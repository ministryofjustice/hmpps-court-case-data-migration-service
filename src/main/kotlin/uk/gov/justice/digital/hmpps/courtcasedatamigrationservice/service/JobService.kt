package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant
import java.util.*

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

    val minId = sourceJdbcTemplate.queryForObject(this.minQuery, Long::class.java)
    val maxId = sourceJdbcTemplate.queryForObject(this.maxQuery, Long::class.java)

    log.info("Retrieved min ID: $minId")
    log.info("Retrieved max ID: $maxId")

    if (minId == null || maxId == null) {
      throw IllegalArgumentException("MinId and MaxId must be provided and non-null")
    }
    if (batchSize <= 0) {
      throw IllegalArgumentException("Batch size must be greater than 0")
    }

    val total = maxId - minId + 1
    if (total <= 0) {
      throw IllegalStateException("Invalid ID range: minId=$minId, maxId=$maxId")
    }

    val baseChunkSize = total / batchSize
    val remainder = total % batchSize

    var currentMin = minId
    for (i in 0 until batchSize) {
      val extra = if (i < remainder) 1 else 0
      val chunkSize = baseChunkSize + extra
      val chunkMax = currentMin!! + chunkSize - 1
      log.info("---------------------------------------------------------------\n")
      log.info("Launching job chunk $i with range: [$currentMin to $chunkMax]")

      val params = JobParametersBuilder()
        .addString("run.id", UUID.randomUUID().toString())
        .addLong("minId", currentMin)
        .addLong("maxId", chunkMax)
        .addLong("batchSize", batchSize.toLong())
        .addLong("currentBatchCount", i.toLong())
        .toJobParameters()

      val jobExecution = jobLauncher.run(job, params)

      if (jobExecution.status == BatchStatus.FAILED || jobExecution.exitStatus.exitCode == ExitStatus.FAILED.exitCode) {
        log.error("Stopping job chain due to failure.")
        break
      }

      currentMin = chunkMax + 1
    }

    log.info("---------------------------------------------------------------\n")

    afterJob()
    ResponseEntity.ok("$jobName job completed. Check logs for details.")
  } catch (ex: Exception) {
    log.error("Error running $jobName job", ex)
    ResponseEntity.internalServerError().body("$jobName job failed: ${ex.message}")
  }

  private fun beforeJob() {
    startTime = Instant.now()
    log.info("Starting $jobName job at $startTime")
  }

  private fun afterJob() {
    val endTime = Instant.now()
    val duration = Duration.between(startTime, endTime)
    log.info("Finished $jobName job at $endTime")
    log.info("Total duration: ${duration.toMinutes()} minutes and ${duration.seconds % 60} seconds")
  }
}
