package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService

class JobSchedulerTest {

  private lateinit var jobService: JobService
  private lateinit var schedulingConfigRepository: SchedulingConfigRepository
  private lateinit var jobScheduler: JobScheduler

  @BeforeEach
  fun setup() {
    jobService = mock()
    schedulingConfigRepository = mock()
    jobScheduler = JobScheduler(jobService, schedulingConfigRepository, JobType.CASE)
  }

  @Test
  fun `should run job when single job enabled and job is enabled`() {
    whenever(schedulingConfigRepository.countEnabledJobs()).thenReturn(1)
    whenever(schedulingConfigRepository.isJobEnabled("CASE")).thenReturn(true)

    jobScheduler.runJob()

    verify(jobService).runJob()
    verify(schedulingConfigRepository).isJobEnabled("CASE")
  }

  @Test
  fun `should not run job when job is disabled`() {
    whenever(schedulingConfigRepository.countEnabledJobs()).thenReturn(1)
    whenever(schedulingConfigRepository.isJobEnabled("CASE")).thenReturn(false)

    jobScheduler.runJob()

    verify(jobService, never()).runJob()
  }

  @Test
  fun `should skip job when multiple jobs enabled`() {
    whenever(schedulingConfigRepository.countEnabledJobs()).thenReturn(3)

    jobScheduler.runJob()

    verify(jobService, never()).runJob()
    verify(schedulingConfigRepository, never()).isJobEnabled(any())
  }

  @Test
  fun `should handle exception thrown from repository`() {
    whenever(schedulingConfigRepository.countEnabledJobs()).thenThrow(RuntimeException("DB down"))

    assertThatCode { jobScheduler.runJob() }
      .doesNotThrowAnyException()

    verify(jobService, never()).runJob()
  }

  @Test
  fun `should handle exception thrown from jobService`() {
    whenever(schedulingConfigRepository.countEnabledJobs()).thenReturn(1)
    whenever(schedulingConfigRepository.isJobEnabled("CASE")).thenReturn(true)
    whenever(jobService.runJob()).thenThrow(RuntimeException("job failed"))

    assertThatCode { jobScheduler.runJob() }
      .doesNotThrowAnyException()

    verify(jobService).runJob()
  }
}
