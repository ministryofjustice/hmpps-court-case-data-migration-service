package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

@RestController
class JobController(private val jobLauncher: JobLauncher,
                    private val job: Job) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(this::class.java)
  }

  @GetMapping("/run-job")
  fun runJob(): ResponseEntity<String> {
    return try {

      log.info("Starting job")

      val params: JobParameters = JobParametersBuilder()
        .addString("run.id", Instant.now().toString())
        .toJobParameters()

      val jobExecution = jobLauncher.run(job, params)

      ResponseEntity.ok("Job ${jobExecution.jobInstance.jobName} started with status ${jobExecution.status}")
    } catch (ex: Exception) {
      ResponseEntity.internalServerError().body("Job failed to start: ${ex.message}")
    }
  }

}
