package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SOURCE_QUERY
import kotlin.collections.firstOrNull

class DefendantOffenceValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    """SELECT
              hd.id
           FROM
              courtcaseservice.defendant d
           JOIN
              courtcaseservice.hearing_defendant hd ON hd.fk_defendant_id  = d.id
           JOIN
              courtcaseservice.offence o ON o.fk_hearing_defendant_id  = hd.id 
        WHERE hd.id BETWEEN ? AND ? 
        ORDER BY RANDOM() LIMIT ?""",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE hd.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getInt("id"),
      "legacy_offence_id" to rs.getInt("legacy_offence_id"),
      "legacy_defendant_id" to rs.getInt("legacy_defendant_id"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "deleted" to rs.getBoolean("deleted"),
      "version" to rs.getInt("version"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? {
    val sourceRecord = fetchSourceRecord(id) ?: return null

    val sourceOffenceId = sourceRecord["legacy_offence_id"]
    val sourceDefendantId = sourceRecord["legacy_defendant_id"]

    return targetJdbcTemplate.query(
      """
            select 
            id, 
            legacy_offence_id,
            legacy_defendant_id,
            created_at,
            created_by,
            updated_at,
            updated_by,
            is_deleted,
            version
            from hmpps_court_case_service.defendant_offence
            where legacy_offence_id = ? and legacy_defendant_id = ?
      """.trimIndent(),
      arrayOf(sourceOffenceId, sourceDefendantId),
    ) { rs, _ ->
      mapOf(
        "legacy_offence_id" to rs.getInt("legacy_offence_id"),
        "legacy_defendant_id" to rs.getInt("legacy_defendant_id"),
        "created_at" to rs.getTimestamp("created_at"),
        "created_by" to rs.getString("created_by"),
        "updated_at" to rs.getTimestamp("updated_at"),
        "updated_by" to rs.getString("updated_by"),
        "is_deleted" to rs.getBoolean("is_deleted"),
        "version" to rs.getInt("version"),
      )
    }.firstOrNull()
  }

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

    compare("legacy_offence_id", "legacy_offence_id", "Offence ID")
    compare("legacy_defendant_id", "legacy_defendant_id", "Defendant ID")
    compare("created", "created_at", "Created")
    compare("created_by", "created_by", "Created by")
    compare("last_updated", "updated_at", "Last updated")
    compare("last_updated_by", "updated_by", "Last updated by")
    compare("deleted", "is_deleted", "Deleted")
    compare("version", "version", "Version")

    return errors
  }
}
