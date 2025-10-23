package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate

class SchedulingConfigRepositoryTest {

  private lateinit var jdbcTemplate: JdbcTemplate
  private lateinit var repository: SchedulingConfigRepository

  @BeforeEach
  fun setup() {
    jdbcTemplate = mock()
    repository = SchedulingConfigRepository(jdbcTemplate)
  }

  @Test
  fun `should return true when job is enabled`() {
    whenever(
      jdbcTemplate.queryForObject(any<String>(), eq(Boolean::class.java), eq("CASE")),
    ).thenReturn(true)

    val result = repository.isJobEnabled("CASE")

    assertThat(result).isTrue()
    verify(jdbcTemplate).queryForObject(any<String>(), eq(Boolean::class.java), eq("CASE"))
  }

  @Test
  fun `should return false when job is disabled`() {
    whenever(
      jdbcTemplate.queryForObject(any<String>(), eq(Boolean::class.java), eq("CASE")),
    ).thenReturn(false)

    val result = repository.isJobEnabled("CASE")

    assertThat(result).isFalse()
  }

  @Test
  fun `should return false when query result is null`() {
    whenever(
      jdbcTemplate.queryForObject(any<String>(), eq(Boolean::class.java), eq("CASE")),
    ).thenReturn(null)

    val result = repository.isJobEnabled("CASE")

    assertThat(result).isFalse()
  }

  @Test
  fun `should return false and log warning when no job found`() {
    whenever(
      jdbcTemplate.queryForObject(any<String>(), eq(Boolean::class.java), eq("MISSING_JOB")),
    ).thenThrow(EmptyResultDataAccessException(1))

    val result = assertDoesNotThrow {
      repository.isJobEnabled("MISSING_JOB")
    }

    assertThat(result).isFalse()
  }

  @Test
  fun `should return number of enabled jobs`() {
    whenever(
      jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)),
    ).thenReturn(2)

    val count = repository.countEnabledJobs()

    assertThat(count).isEqualTo(2)
    verify(jdbcTemplate).queryForObject(any<String>(), eq(Int::class.java))
  }

  @Test
  fun `should return 0 when count query returns null`() {
    whenever(
      jdbcTemplate.queryForObject(any<String>(), eq(Int::class.java)),
    ).thenReturn(null)

    val count = repository.countEnabledJobs()

    assertThat(count).isEqualTo(0)
  }
}
