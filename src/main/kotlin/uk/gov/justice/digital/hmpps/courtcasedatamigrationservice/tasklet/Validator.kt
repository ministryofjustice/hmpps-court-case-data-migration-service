
package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

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
    val sortedTarget = targetList.sortedBy { it["legacyID"].toString() }

    for (i in sortedSource.indices) {
      val source = sortedSource.getOrNull(i)
      val target = sortedTarget.getOrNull(i)

      if (source == null || target == null) continue

      for ((sourceField, targetField, label) in fieldMappings) {
        val sourceValue = source[sourceField]
        val targetValue = target[targetField]

        val parsedSourceDate = normalizeDate(sourceValue)
        val parsedTargetDate = normalizeDate(targetValue)

        val mismatch = when {
          parsedSourceDate != null && parsedTargetDate != null ->
            parsedSourceDate != parsedTargetDate
          else -> sourceValue != targetValue
        }

        if (mismatch) {
          errors.add("$entityName field '$label' mismatch for ID $id (index $i): '$sourceValue' vs '$targetValue'")
        }
      }
    }

    return errors
  }

  private fun normalizeDate(value: Any?): Instant? = try {
    val text = value.toString()
    val instant = try {
      OffsetDateTime.parse(text, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        .toInstant()
    } catch (_: Exception) {
      LocalDateTime.parse(text, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        .atOffset(ZoneOffset.UTC)
        .toInstant()
    }
    instant.truncatedTo(ChronoUnit.SECONDS)
  } catch (_: Exception) {
    null
  }
}
