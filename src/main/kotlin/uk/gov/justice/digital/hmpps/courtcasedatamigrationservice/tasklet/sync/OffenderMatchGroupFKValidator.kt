package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants

class OffenderMatchGroupFKValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : SyncValidator() {

  val fetchSourceIDsQuery = """
        SELECT 
              omg.id
              ${OffenderMatchGroupConstants.BASE_FROM_CLAUSE}
            WHERE omg.id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?
    """

  val fetchSourceRecordQuery = """
        SELECT 
              omg.id,
              d.id AS legacy_defendant_id,
              cc.id AS legacy_prosecution_case_id
        ${OffenderMatchGroupConstants.BASE_FROM_CLAUSE}
        WHERE omg.id = ?
    """

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    fetchSourceIDsQuery.trimIndent(),
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    fetchSourceRecordQuery.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "legacy_defendant_id" to rs.getLong("legacy_defendant_id"),
      "legacy_prosecution_case_id" to rs.getLong("legacy_prosecution_case_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>? = targetJdbcTemplate.query(
    """
    SELECT COUNT(*)
    FROM hmpps_court_case_service.offender_match_group omg 
    WHERE omg.legacy_defendant_id = ? 
    AND omg.legacy_prosecution_case_id = ?;
    """.trimIndent(),
    arrayOf(map["legacy_defendant_id"], map["legacy_prosecution_case_id"]),
  ) { rs, _ ->
    mapOf(
      "count" to rs.getLong("count"),
    )
  }.firstOrNull()
}
