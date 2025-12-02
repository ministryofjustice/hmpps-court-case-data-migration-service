package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice

import org.assertj.core.api.Assertions.assertThat
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.UUID

object TestUtils {

  fun isValueUUID(uuid: String): Boolean = try {
    UUID.fromString(uuid)
    true
  } catch (_: IllegalArgumentException) {
    false
  }

  fun assertDateTimeEquals(actual: String?, expectedTimestamp: Timestamp?, zone: ZoneId = ZoneId.of("Europe/London")) {
    requireNotNull(actual) { "Actual date string cannot be null" }
    requireNotNull(expectedTimestamp) { "Expected timestamp cannot be null" }

    val actualOffset = OffsetDateTime.parse(actual)
    val expectedOffset = expectedTimestamp.toInstant().atZone(zone).toOffsetDateTime()

    assertThat(actualOffset).isEqualTo(expectedOffset)
  }

  fun assertLocalDateTimeEquals(actual: String?, expected: String?) {
    requireNotNull(actual) { "Actual date string cannot be null" }
    requireNotNull(expected) { "Expected date string cannot be null" }

    val actualInstant = OffsetDateTime.parse(actual).toInstant().truncatedTo(ChronoUnit.MILLIS)
    val expectedInstant = LocalDateTime.parse(expected)
      .atZone(ZoneId.of("UTC"))
      .toInstant()
      .truncatedTo(ChronoUnit.MILLIS)

    assertThat(actualInstant).isEqualTo(expectedInstant)
  }
}
