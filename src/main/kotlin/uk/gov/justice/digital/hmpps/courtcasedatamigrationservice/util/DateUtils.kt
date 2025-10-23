package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

object DateUtils {

  private val targetZoneId: ZoneId = ZoneId.of("Europe/London")
  private val formatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  // Normalize ISO string (with or without timezone) to ISO 8601 with offset
  fun normalizeIsoDateTime(input: String?): String? {
    if (input == null) return null

    return try {
      val offsetDateTime = OffsetDateTime.parse(input)
      offsetDateTime.atZoneSameInstant(targetZoneId).toOffsetDateTime().format(formatter)
    } catch (e: DateTimeParseException) {
      val localDateTime = LocalDateTime.parse(input)
      localDateTime.atZone(targetZoneId).toOffsetDateTime().format(formatter)
    }
  }

  // Normalize java.sql.Timestamp to ISO 8601 with offset
  fun normalizeIsoDateTime(timestamp: Timestamp?): String? {
    if (timestamp == null) return null
    return timestamp.toInstant()
      .atZone(targetZoneId)
      .toOffsetDateTime()
      .format(formatter)
  }
}
