package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.SOURCE_QUERY
import java.util.UUID

class CaseCommentValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.case_comments WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE cc.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "case_id" to rs.getString("case_id"),
      "defendant_id" to rs.getObject("defendant_id", UUID::class.java),
      "author" to rs.getString("author"),
      "comment" to rs.getString("comment"),
      "is_draft" to rs.getBoolean("is_draft"),
      "legacy" to rs.getBoolean("legacy"),
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
            legacy_defendant_id,
            legacy_case_id,
            author,
            comment,
            is_draft,
            is_legacy,
            created_at,
            created_by,
            updated_at ,
            updated_by,
            is_deleted,
            version
            from hmpps_court_case_service.case_comment
            WHERE legacy_id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "legacy_id" to rs.getLong("legacy_id"),
      "legacy_defendant_id" to rs.getString("legacy_defendant_id"),
      "legacy_case_id" to rs.getString("legacy_case_id"),
      "author" to rs.getString("author"),
      "comment" to rs.getString("comment"),
      "is_draft" to rs.getBoolean("is_draft"),
      "is_legacy" to rs.getBoolean("is_legacy"),
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
      var sourceValue = source[fieldSource]
      val targetValue = target[fieldTarget]
      if (sourceValue != null && sourceValue is UUID) {
        sourceValue = sourceValue.toString()
      }
      if (sourceValue != targetValue) {
        errors.add("$label mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }

    compare("id", "legacy_id", "Case Comment ID")
    compare("defendant_id", "legacy_defendant_id", "Defendant ID")
    compare("case_id", "legacy_case_id", "Case ID")
    compare("author", "author", "Author")
    compare("comment", "comment", "Comment")
    compare("is_draft", "is_draft", "Is Draft")
    compare("legacy", "is_legacy", "Is Legacy")
    compare("created", "created_at", "Created")
    compare("created_by", "created_by", "Created by")
    compare("last_updated", "updated_at", "Last updated")
    compare("last_updated_by", "updated_by", "Last updated by")
    compare("deleted", "is_deleted", "Deleted")
    compare("version", "version", "Version")

    return errors
  }
}
