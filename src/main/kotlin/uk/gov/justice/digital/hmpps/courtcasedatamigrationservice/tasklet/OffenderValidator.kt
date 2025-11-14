package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderConstants.SOURCE_QUERY

class OffenderValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.offender WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE o.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "suspended_sentence_order" to rs.getBoolean("suspended_sentence_order"),
      "breach" to rs.getBoolean("breach"),
      "awaiting_psr" to rs.getBoolean("awaiting_psr"),
      "probation_status" to rs.getString("probation_status"),
      "pre_sentence_activity" to rs.getBoolean("pre_sentence_activity"),
      "previously_known_termination_date" to rs.getDate("previously_known_termination_date"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "deleted" to rs.getBoolean("deleted"),
      "version" to rs.getInt("version"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
          select 
          legacy_id,
          suspended_sentence_order,
          breach,
          awaiting_psr,
          probation_status,
          pre_sentence_activity,
          previously_known_termination_date,
          created_at,
          created_by,
          updated_at ,
          updated_by,
          is_deleted,
          version
          from hmpps_court_case_service.offender
            WHERE legacy_id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "legacy_id" to rs.getLong("legacy_id"),
      "suspended_sentence_order" to rs.getBoolean("suspended_sentence_order"),
      "breach" to rs.getBoolean("breach"),
      "awaiting_psr" to rs.getBoolean("awaiting_psr"),
      "probation_status" to rs.getString("probation_status"),
      "pre_sentence_activity" to rs.getBoolean("pre_sentence_activity"),
      "previously_known_termination_date" to rs.getDate("previously_known_termination_date"),
      "created_at" to rs.getTimestamp("created_at"),
      "created_by" to rs.getString("created_by"),
      "updated_at" to rs.getTimestamp("updated_at"),
      "updated_by" to rs.getString("updated_by"),
      "is_deleted" to rs.getBoolean("is_deleted"),
      "version" to rs.getInt("version"),
    )
  }.firstOrNull()

  override fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String> {
    val errors = mutableListOf<String>()
    val id = source["id"]

    fun compare(fieldSource: String, fieldTarget: String, label: String = fieldSource) {
      val sourceValue = source[fieldSource]
      val targetValue = target[fieldTarget]
      if (sourceValue != targetValue) {
        errors.add("$label mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }

    compare("id", "legacy_id", "Offender ID")
    compare("suspended_sentence_order", "suspended_sentence_order", "Suspended sentence order")
    compare("breach", "breach", "Breach")
    compare("awaiting_psr", "awaiting_psr", "Awaiting PSR")
    compare("probation_status", "probation_status", "Probation status")
    compare("pre_sentence_activity", "pre_sentence_activity", "Pre sentence activity")
    compare("previously_known_termination_date", "previously_known_termination_date", "Previously known termination date")
    compare("created", "created_at", "Created")
    compare("created_by", "created_by", "Created by")
    compare("last_updated", "updated_at", "Last updated")
    compare("last_updated_by", "updated_by", "Last updated by")
    compare("deleted", "is_deleted", "Deleted")
    compare("version", "version", "Version")

    return errors
  }
}
