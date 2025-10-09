package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate

class DefendantValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.defendant WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """
            SELECT id, name ->> 'forename1' AS forename1, name ->> 'surname' AS surname, crn
            FROM courtcaseservice.defendant
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "forename1" to rs.getString("forename1"),
      "surname" to rs.getString("surname"),
      "crn" to rs.getString("crn"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
            SELECT id, person ->> 'firstName' AS firstName, person ->> 'lastName' AS lastName, crn
            FROM hmpps_court_case_service.defendant
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "firstName" to rs.getString("firstName"),
      "lastName" to rs.getString("lastName"),
      "crn" to rs.getString("crn"),
    )
  }.firstOrNull()

  override fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String> {
    val errors = mutableListOf<String>()
    val id = source["id"]

    if (source["forename1"] != target["firstName"]) {
      errors.add("First name mismatch for ID $id: '${source["forename1"]}' vs '${target["firstName"]}'")
    }

    if (source["surname"] != target["lastName"]) {
      errors.add("Last name mismatch for ID $id: '${source["surname"]}' vs '${target["lastName"]}'")
    }

    if (source["crn"] != target["crn"]) {
      errors.add("CRN mismatch for ID $id: '${source["crn"]}' vs '${target["crn"]}'")
    }

    return errors
  }
}
