package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate

class CaseFkInCaseCommentValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : SyncValidator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = targetJdbcTemplate.queryForList(
    """
      SELECT pc.legacy_id 
      FROM hmpps_court_case_service.prosecution_case pc JOIN hmpps_court_case_service.case_comment cc ON (pc.case_id = cc.legacy_case_id)
      WHERE pc.legacy_id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?
    """.trimMargin(),
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """SELECT 
             cco.id as case_comments_id, 
             cca.case_id
             FROM courtcaseservice.court_case cca JOIN courtcaseservice.case_comments cco ON (cca.case_id = cco.case_id)
             WHERE cca.id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "case_comments_id" to rs.getLong("case_comments_id"),
      "case_id" to rs.getString("case_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>? = targetJdbcTemplate.query(
    """
       SELECT count(*)
       FROM hmpps_court_case_service.case_comment cc
       WHERE cc.legacy_id = ?
       AND cc.legacy_case_id = ?
    """.trimIndent(),
    arrayOf(map["case_comments_id"], map["case_id"]),
  ) { rs, _ ->
    mapOf(
      "count" to rs.getLong("count"),
    )
  }.firstOrNull()
}
