package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SOURCE_QUERY

class OffenderMatchGroupValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    """
      select omg.id from courtcaseservice.offender_match_group omg 	join courtcaseservice.court_case cc on (omg.case_id = cc.case_id)
      join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
      where omg.id between ? and ? order by random() limit ?
    """.trimIndent(),
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE omg.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "case_id" to rs.getInt("case_id"),
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
        prosecution_case_id,
        defendant_id,
        created_at,
        created_by,
        updated_at,
        updated_by,
        is_deleted,
        version
        from hmpps_court_case_service.offender_match_group
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "prosecution_case_id" to rs.getInt("prosecution_case_id"),
      "defendant_id" to rs.getInt("defendant_id"),
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
    compare("case_id", "prosecution_case_id", "Prosecution Case ID")
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
