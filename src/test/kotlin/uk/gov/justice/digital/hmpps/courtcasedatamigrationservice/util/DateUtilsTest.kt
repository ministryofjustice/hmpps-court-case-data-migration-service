package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

class DateUtilsTest {

  @Test
  fun `should normalize ISO string with offset`() {
    val input = "2023-10-23T10:15:30+01:00"
    val result = DateUtils.normalizeIsoDateTime(input)
    assertThat(result).isEqualTo("2023-10-23T10:15:30+01:00")
  }

  @Test
  fun `should normalize ISO string without offset`() {
    val input = "2023-10-23T10:15:30"
    val result = DateUtils.normalizeIsoDateTime(input)
    assertThat(result).isEqualTo("2023-10-23T10:15:30+01:00")
  }

  @Test
  fun `should normalize ISO string with offset and microseconds`() {
    val input = "2025-07-28T09:08:46.720893"
    val result = DateUtils.normalizeIsoDateTime(input)
    assertThat(result).isEqualTo("2025-07-28T09:08:46.720893+01:00")
  }

  @Test
  fun `should return null for null string input`() {
    val result = DateUtils.normalizeIsoDateTime(null as String?)
    assertThat(result).isNull()
  }

  @Test
  fun `should normalize Timestamp to ISO string`() {
    val timestamp = Timestamp.valueOf(LocalDateTime.of(2023, 10, 23, 10, 15, 30))
    val result = DateUtils.normalizeIsoDateTime(timestamp)
    assertThat(result).isEqualTo("2023-10-23T10:15:30+01:00")
  }

  @Test
  fun `should return null for null Timestamp input`() {
    val result = DateUtils.normalizeIsoDateTime(null as Timestamp?)
    assertThat(result).isNull()
  }

  @Test
  fun `should handle UTC OffsetDateTime and convert to London time`() {
    val input = OffsetDateTime.of(2023, 10, 23, 9, 15, 30, 0, ZoneOffset.UTC).toString()
    val result = DateUtils.normalizeIsoDateTime(input)
    assertThat(result).isEqualTo("2023-10-23T10:15:30+01:00")
  }
}
