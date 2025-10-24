package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

abstract class Validator {
  abstract fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long>
  abstract fun fetchSourceRecord(id: Long): Map<String, Any>?
  abstract fun fetchTargetRecord(id: Long): Map<String, Any>?
  abstract fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String>

  val objectMapper = jacksonObjectMapper()

  fun compareJsonLists(
    sourceJson: String?,
    targetJson: String?,
    id: Any?,
    entityName: String,
    fieldMappings: List<Triple<String, String, String>>,
  ): List<String> {
    val errors = mutableListOf<String>()

    val sourceList: List<Map<String, Any?>> = try {
      objectMapper.readValue(sourceJson ?: "[]")
    } catch (e: Exception) {
      errors.add("Failed to parse source $entityName for ID $id")
      return errors
    }

    val targetList: List<Map<String, Any?>> = try {
      objectMapper.readValue(targetJson ?: "[]")
    } catch (e: Exception) {
      errors.add("Failed to parse target $entityName for ID $id")
      return errors
    }

    if (sourceList.size != targetList.size) {
      errors.add("$entityName count mismatch for ID $id: ${sourceList.size} vs ${targetList.size}")
    }

    val sortedSource = sourceList.sortedBy { it["id"].toString() }
    val sortedTarget = targetList.sortedBy { it["id"].toString() }

    for (i in sortedSource.indices) {
      val source = sortedSource.getOrNull(i)
      val target = sortedTarget.getOrNull(i)

      if (source == null || target == null) continue

      for ((sourceField, targetField, label) in fieldMappings) {
        val sourceValue = source[sourceField]
        val targetValue = target[targetField]

        val parsedSourceDate = parseDate(sourceValue)
        val parsedTargetDate = parseDate(targetValue)

        val mismatch = when {
          parsedSourceDate != null && parsedTargetDate != null ->
            !parsedSourceDate.isEqual(parsedTargetDate)
          else -> sourceValue != targetValue
        }

        if (mismatch) {
          errors.add("$entityName field '$label' mismatch for ID $id (index $i): '$sourceValue' vs '$targetValue'")
        }
      }
    }

    return errors
  }

  private fun parseDate(targetValue: Any?): ZonedDateTime? = try {
    val zone = ZoneId.of("Europe/London")
    val text = targetValue.toString()
    when {
      text.endsWith("Z") || text.contains("+") || text.matches(Regex(""".*\dT\d{2}:\d{2}:\d{2}.*-\d{2}:\d{2}""")) -> {
        try {
          ZonedDateTime.parse(text, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        } catch (_: Exception) {
          OffsetDateTime.parse(text, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toZonedDateTime()
        }
      }
      else -> {
        LocalDateTime.parse(text, DateTimeFormatter.ISO_LOCAL_DATE_TIME).atZone(zone)
      }
    }
  } catch (e: Exception) {
    null
  }
}
