package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID

class DefendantFkInCaseCommentValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : SyncValidator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = targetJdbcTemplate.queryForList(
    """
      |SELECT d.legacy_id 
      |FROM hmpps_court_case_service.defendant d JOIN hmpps_court_case_service.case_comment cc ON (d.defendant_id = cc.legacy_defendant_id) 
      |WHERE d.legacy_id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?
    """.trimMargin(),
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """ SELECT 
             cc.id as case_comments_id, 
             d.defendant_id
             FROM courtcaseservice.defendant d JOIN courtcaseservice.case_comments cc ON (d.defendant_id = cc.defendant_id)
             WHERE d.id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "case_comments_id" to rs.getLong("case_comments_id"),
      "defendant_id" to rs.getObject("defendant_id", UUID::class.java),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>? = targetJdbcTemplate.query(
    """
       SELECT count(*)
       FROM hmpps_court_case_service.case_comment cc
       WHERE cc.legacy_id = ?
       AND cc.legacy_defendant_id = ? 
    """.trimIndent(),
    arrayOf(map["case_comments_id"], map["defendant_id"]),
  ) { rs, _ ->
    mapOf(
      "count" to rs.getLong("count"),
    )
  }.firstOrNull()
}
