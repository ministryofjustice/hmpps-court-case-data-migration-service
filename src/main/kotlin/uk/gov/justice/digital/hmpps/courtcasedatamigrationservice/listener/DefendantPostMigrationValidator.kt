package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener

import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.query
import kotlin.random.Random

class DefendantPostMigrationValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
  private val minQuery: String,
  private val maxQuery: String,
) : JobExecutionListener {

  private val log = LoggerFactory.getLogger(DefendantPostMigrationValidator::class.java)

  override fun afterJob(jobExecution: JobExecution) {
    validateSampleDefendants()
  }

  fun validateSampleDefendants(sampleSize: Int = 10): List<String> {
    val minId = sourceJdbcTemplate.queryForObject(this.minQuery, Long::class.java)
    val maxId = sourceJdbcTemplate.queryForObject(this.maxQuery, Long::class.java)

    if (minId == null || maxId == null) {
      log.warn("Min or Max ID is null. Skipping validation.")
      return emptyList()
    }

    log.info("Retrieved min ID: $minId")
    log.info("Retrieved max ID: $maxId")

    val validatedRecords = mutableListOf<Map<String, Any>>()
    val attemptedIds = mutableSetOf<Long>()

    while (validatedRecords.size < sampleSize && attemptedIds.size < (maxId - minId)) {
      val id = Random.nextLong(minId, maxId + 1)
      if (id in attemptedIds) continue
      attemptedIds.add(id)

      val sourceRecords = sourceJdbcTemplate.query(
        """
      SELECT 
        id, 
        name ->> 'forename1' AS forename1, 
        name ->> 'surname' AS surname, 
        crn 
      FROM courtcaseservice.defendant 
      WHERE id = ?
        """.trimIndent(),
        id,
      ) { rs, _ ->
        mapOf(
          "id" to rs.getInt("id"),
          "forename1" to rs.getString("forename1"),
          "surname" to rs.getString("surname"),
          "crn" to rs.getString("crn"),
        )
      }

      if (sourceRecords.isNotEmpty()) {
        validatedRecords.addAll(sourceRecords)
      }
    }

    val errors = mutableListOf<String>()

    validatedRecords.forEach { source ->
      val target = targetJdbcTemplate.queryForList(
        """
      SELECT id, person ->> 'firstName' AS firstName, person ->> 'lastName' AS lastName, crn 
      FROM hmpps_court_case_service.defendant 
      WHERE id = ?
        """.trimIndent(),
        source["id"],
      ).firstOrNull()

      if (target == null) {
        errors.add("Missing target record for ID ${source["id"]}")
        return@forEach
      }

      if (source["forename1"] != target["firstName"]) {
        errors.add("First name mismatch for ID ${source["id"]}: '${source["forename1"]}' vs '${target["firstName"]}'")
      }

      if (source["surname"] != target["lastName"]) {
        errors.add("Last name mismatch for ID ${source["id"]}: '${source["surname"]}' vs '${target["lastName"]}'")
      }

      if (source["crn"] != target["crn"]) {
        errors.add("CRN mismatch for ID ${source["id"]}: '${source["crn"]}' vs '${target["crn"]}'")
      }
    }

    if (errors.isEmpty()) {
      log.info("✅ All sample records passed validation.")
    } else {
      log.warn("❌ Validation errors found:\n${errors.joinToString("\n")}")
    }

    return errors
  }
}
