package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.Validator
import java.sql.Date
import java.sql.Timestamp

class DefendantOffenderFKValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = targetJdbcTemplate.queryForList(
    "SELECT legacy_id FROM hmpps_court_case_service.offender WHERE legacy_id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """
        SELECT MAX(id) AS id, fk_offender_id
        FROM courtcaseservice.defendant
        WHERE fk_offender_id = ?
        GROUP BY fk_offender_id;
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "fk_offender_id" to rs.getLong("fk_offender_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
    SELECT
        max(legacy_id) AS legacy_id, 
        legacy_offender_id
    FROM hmpps_court_case_service.defendant
    WHERE legacy_offender_id = ?
    GROUP BY legacy_offender_id;
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "legacy_id" to rs.getLong("legacy_id"),
      "legacy_offender_id" to rs.getLong("legacy_offender_id"),
    )
  }.firstOrNull()

  override fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String> {
    val errors = mutableListOf<String>()
    val id = source["id"]

    fun compare(fieldSource: String, fieldTarget: String, label: String = fieldSource) {
      fun normalizeValue(value: Any?): String? = when (value) {
        null -> null
        is String -> if (value.equals("null", ignoreCase = true)) null else value
        is Date -> value.toLocalDate().toString()
        is Timestamp -> value.toLocalDateTime().toString()
        else -> value.toString()
      }

      val sourceValue = normalizeValue(source[fieldSource])
      val targetValue = normalizeValue(target[fieldTarget])
      if (sourceValue != targetValue) {
        errors.add("$label mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }

    compare("id", "legacy_id", "Legacy Defendant ID")
    compare("fk_offender_id", "legacy_offender_id", "Legacy Offender ID")

    return errors
  }
}
