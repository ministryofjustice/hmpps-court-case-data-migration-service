package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate

class DefendantFKValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : SyncValidator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = targetJdbcTemplate.queryForList(
    "SELECT legacy_offender_id FROM hmpps_court_case_service.defendant WHERE legacy_offender_id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """SELECT max(id) as legacy_id, d.fk_offender_id as legacy_offender_id 
            FROM courtcaseservice.defendant d
            WHERE d.fk_offender_id = ?
            GROUP BY d.fk_offender_id
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "legacy_id" to rs.getLong("legacy_id"),
      "legacy_offender_id" to rs.getLong("legacy_offender_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>? = targetJdbcTemplate.query(
    """
    SELECT COUNT(*)
    FROM hmpps_court_case_service.defendant d
    WHERE d.legacy_id = ?
    AND d.legacy_offender_id = ?;
    """.trimIndent(),
    arrayOf(map["legacy_id"], map["legacy_offender_id"]),
  ) { rs, _ ->
    mapOf(
      "count" to rs.getLong("count"),
    )
  }.firstOrNull()
}
