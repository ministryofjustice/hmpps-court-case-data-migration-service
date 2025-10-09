package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate

class OffenceValidationStrategy(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : PostMigrationValidationStrategy {

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """
            SELECT id, offence_code, title, act
            FROM courtcaseservice.offence
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "offence_code" to rs.getString("offence_code"),
      "title" to rs.getString("title"),
      "act" to rs.getString("act"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
            SELECT id, code, title, legislation 
            FROM hmpps_court_case_service.offence
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "code" to rs.getString("code"),
      "title" to rs.getString("title"),
      "legislation" to rs.getString("legislation"),
    )
  }.firstOrNull()

  override fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String> {
    val errors = mutableListOf<String>()
    val id = source["id"]

    if (source["offence_code"] != target["code"]) {
      errors.add("Offence code mismatch for ID $id: '${source["offence_code"]}' vs '${target["code"]}'")
    }

    if (source["title"] != target["title"]) {
      errors.add("Title mismatch for ID $id: '${source["title"]}' vs '${target["title"]}'")
    }

    if (source["act"] != target["legislation"]) {
      errors.add("Act (or Legislation) mismatch for ID $id: '${source["act"]}' vs '${target["legislation"]}'")
    }

    if (source["act"] != "hello") {
      errors.add("Act (or Legislation) mismatch for ID $id: '${source["act"]}' vs hello")
    }

    return errors
  }
}
