package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants

class OffenderMatchFKValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : SyncValidator() {

  val fetchSourceIDsQuery = """
        SELECT 
              om.id
              ${OffenderMatchConstants.BASE_FROM_CLAUSE}
            WHERE om.id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?
    """

  val fetchSourceRecordQuery = """
        SELECT 
              om.id,
              om.group_id AS legacy_offender_match_group_id,
              d.fk_offender_id AS legacy_offender_id
        ${OffenderMatchConstants.BASE_FROM_CLAUSE}
        WHERE om.id = ?
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
      "legacy_offender_match_group_id" to rs.getLong("legacy_offender_match_group_id"),
      "legacy_offender_id" to rs.getLong("legacy_offender_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>? = targetJdbcTemplate.query(
    """
    SELECT COUNT(*)
    FROM hmpps_court_case_service.offender_match om 
    WHERE om.legacy_offender_match_group_id = ? 
    AND om.legacy_offender_id = ?;
    """.trimIndent(),
    arrayOf(map["legacy_offender_match_group_id"], map["legacy_offender_id"]),
  ) { rs, _ ->
    mapOf(
      "count" to rs.getLong("count"),
    )
  }.firstOrNull()
}
