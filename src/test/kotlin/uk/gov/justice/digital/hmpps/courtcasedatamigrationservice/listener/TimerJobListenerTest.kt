package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import java.time.Instant
import java.time.temporal.ChronoUnit

class TimerJobListenerTest {

  private lateinit var listener: TimerJobListener
  private lateinit var jobExecution: JobExecution

  @BeforeEach
  fun setup() {
    listener = TimerJobListener()
    jobExecution = JobExecution(1L)
    jobExecution.jobInstance = JobInstance(1L, "TestJob")
  }

  @Test
  fun `should set start time before job`() {
    listener.beforeJob(jobExecution)

    assertThat(jobExecution.jobInstance.jobName).isEqualTo("TestJob")
  }

  @Test
  fun `should log duration after job`() {
    listener.beforeJob(jobExecution)

    Thread.sleep(100)

    listener.afterJob(jobExecution)

    assertThat(jobExecution.jobInstance.jobName).isEqualTo("TestJob")
  }

  @Test
  fun `should calculate correct duration`() {
    listener.beforeJob(jobExecution)

    val start = Instant.now()
    val end = start.plus(2, ChronoUnit.MINUTES).plus(30, ChronoUnit.SECONDS)

    val duration = java.time.Duration.between(start, end)

    assertThat(duration.toMinutes()).isEqualTo(2)
    assertThat(duration.seconds % 60).isEqualTo(30)
  }
}
