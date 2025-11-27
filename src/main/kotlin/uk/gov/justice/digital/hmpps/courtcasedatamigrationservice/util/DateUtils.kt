
package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

object DateUtils {

  private val targetZoneId: ZoneId = ZoneId.of("Europe/London")
  private val formatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  fun normalizeIsoDateTime(input: String?): String? {
    if (input == null) return null
    return try {
      val offsetDateTime = OffsetDateTime.parse(input)
      offsetDateTime
        .atZoneSameInstant(targetZoneId)
        .toOffsetDateTime()
//        .truncatedTo(ChronoUnit.SECONDS)
        .format(formatter)
    } catch (_: DateTimeParseException) {
      val localDateTime = LocalDateTime.parse(input)
      localDateTime
        .atZone(ZoneId.of("UTC"))
        .withZoneSameInstant(targetZoneId)
        .toOffsetDateTime()
//        .truncatedTo(ChronoUnit.SECONDS)
        .format(formatter)
    }
  }

  fun normalizeIsoDateTime(timestamp: Timestamp?): String? {
    if (timestamp == null) return null
    return timestamp.toInstant()
      .atZone(targetZoneId)
      .toOffsetDateTime()
//      .truncatedTo(ChronoUnit.SECONDS)
      .format(formatter)
  }
}
