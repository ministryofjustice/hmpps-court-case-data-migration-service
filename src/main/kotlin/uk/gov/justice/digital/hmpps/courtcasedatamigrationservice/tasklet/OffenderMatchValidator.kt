package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SOURCE_QUERY

class OffenderMatchValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    """SELECT om.id from courtcaseservice.offender_match om join courtcaseservice.offender_match_group omg on (om.group_id = omg.id)
    										join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
    										join courtcaseservice.offender o on (d.fk_offender_id = o.id) WHERE om.id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?""",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  // TODO implement missing fields
  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE om.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
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
        created_at,
        created_by,
        updated_at,
        updated_by,
        is_deleted,
        version
        from hmpps_court_case_service.offender_match
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
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

    compare("created", "created_at", "Created")
    compare("created_by", "created_by", "Created by")
    compare("last_updated", "updated_at", "Last updated")
    compare("last_updated_by", "updated_by", "Last updated by")
    compare("deleted", "is_deleted", "Deleted")
    compare("version", "version", "Version")

    return errors
  }
}
