package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SOURCE_QUERY

class DefendantOffenceValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  // TODO update this query
  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.offence WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE o.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getInt("id"),
      "offence_id" to rs.getInt("offence_id"),
      "defendant_id" to rs.getInt("defendant_id"),
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
            id, 
            offence_id,
            defendant_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            is_deleted,
            version
            from hmpps_court_case_service.defendant_offence
            where id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "offence_id" to rs.getString("offence_id"),
      "defendant_id" to rs.getString("defendant_id"),
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

    fun compare(fieldSource: String, fieldTarget: String = fieldSource, label: String = fieldSource) {
      val sourceValue = source[fieldSource]
      val targetValue = target[fieldTarget]
      if (sourceValue != targetValue) {
        errors.add("$label mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }

    compare("id", "id", "ID")
    compare("offence_id", "offence_id", "Offence ID")
    compare("defendant_id", "defendant_id", "Defendant ID")
    compare("created", "created_at", "Created")
    compare("created_by", "created_by", "Created by")
    compare("last_updated", "updated_at", "Last updated")
    compare("last_updated_by", "updated_by", "Last updated by")
    compare("deleted", "is_deleted", "Deleted")
    compare("version", "version", "Version")

    return errors
  }
}
