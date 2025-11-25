package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

import org.springframework.jdbc.core.JdbcTemplate

class DefendantOffenceFKValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : SyncValidator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    """SELECT
        hd.id
     FROM
        courtcaseservice.defendant d
     JOIN
        courtcaseservice.hearing_defendant hd ON hd.fk_defendant_id  = d.id
     JOIN
        courtcaseservice.offence o ON o.fk_hearing_defendant_id  = hd.id 
        WHERE hd.id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?""",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """
     SELECT
        hd.id,
        d.id AS legacy_defendant_id,
        o.id AS legacy_offence_id
     FROM
        courtcaseservice.defendant d
     JOIN
        courtcaseservice.hearing_defendant hd ON hd.fk_defendant_id  = d.id
     JOIN
        courtcaseservice.offence o ON o.fk_hearing_defendant_id  = hd.id
     WHERE hd.id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "legacy_defendant_id" to rs.getLong("legacy_defendant_id"),
      "legacy_offence_id" to rs.getLong("legacy_offence_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>? = targetJdbcTemplate.query(
    """
    SELECT COUNT(*)
    FROM hmpps_court_case_service.defendant_offence do2 
    WHERE do2.legacy_defendant_id = ? 
    AND do2.legacy_offence_id = ?;
    """.trimIndent(),
    arrayOf(map["legacy_defendant_id"], map["legacy_offence_id"]),
  ) { rs, _ ->
    mapOf(
      "count" to rs.getLong("count"),
    )
  }.firstOrNull()
}
