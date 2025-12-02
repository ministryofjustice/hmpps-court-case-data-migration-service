package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenceConstants.SOURCE_QUERY

class OffenceValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.offence WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE o.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      // Offence
      "id" to rs.getLong("id"),
      "fk_hearing_defendant_id" to rs.getLong("fk_hearing_defendant_id"),
      "offence_code" to rs.getString("offence_code"),
      "summary" to rs.getString("summary"),
      "title" to rs.getString("title"),
      "sequence" to rs.getInt("sequence"),
      "act" to rs.getString("act"),
      "list_no" to rs.getInt("list_no"),
      "short_term_custody_predictor_score" to rs.getInt("short_term_custody_predictor_score"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "deleted" to rs.getBoolean("deleted"),
      "version" to rs.getInt("version"),

      // Plea
      "plea_id" to rs.getLong("plea_id"),
      "plea_date" to rs.getTimestamp("plea_date"),
      "plea_value" to rs.getString("plea_value"),
      "plea_created" to rs.getTimestamp("plea_created"),
      "plea_created_by" to rs.getString("plea_created_by"),
      "plea_last_updated" to rs.getTimestamp("plea_last_updated"),
      "plea_last_updated_by" to rs.getString("plea_last_updated_by"),
      "plea_deleted" to rs.getBoolean("plea_deleted"),
      "plea_version" to rs.getInt("plea_version"),

      // Verdict
      "verdict_id" to rs.getLong("verdict_id"),
      "verdict_date" to rs.getTimestamp("verdict_date"),
      "verdict_type_description" to rs.getString("verdict_type_description"),
      "verdict_created" to rs.getTimestamp("verdict_created"),
      "verdict_created_by" to rs.getString("verdict_created_by"),
      "verdict_last_updated" to rs.getTimestamp("verdict_last_updated"),
      "verdict_last_updated_by" to rs.getString("verdict_last_updated_by"),
      "verdict_deleted" to rs.getBoolean("verdict_deleted"),
      "verdict_version" to rs.getInt("verdict_version"),

      // Judicial Results
      "judicial_results" to rs.getString("judicial_results"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
SELECT
  legacy_id, 
  code, 
  title, 
  legislation,
  listing_number,
  sequence,
  short_term_custody_predictor_score,
  wording,
  
  -- Verdict
  verdict ->> 'legacyID' AS legacy_verdict_id,
  verdict ->> 'date' AS verdict_date,
  verdict ->> 'type' AS verdict_type,
  verdict ->> 'version' AS verdict_version,
  verdict ->> 'createdAt' AS verdict_created_at,
  verdict ->> 'createdBy' AS verdict_created_by,
  verdict ->> 'isDeleted' AS verdict_is_deleted,
  verdict ->> 'lastUpdatedAt' AS verdict_last_updated_at,
  verdict ->> 'lastUpdatedBy' AS verdict_last_updated_by,

  -- Plea
  plea ->> 'legacyID' AS legacy_plea_id,
  plea ->> 'date' AS plea_date,
  plea ->> 'value' AS plea_value,
  plea ->> 'version' AS plea_version,
  plea ->> 'createdAt' AS plea_created_at,
  plea ->> 'createdBy' AS plea_created_by,
  plea ->> 'isDeleted' AS plea_is_deleted,
  plea ->> 'lastUpdatedAt' AS plea_last_updated_at,
  plea ->> 'lastUpdatedBy' AS plea_last_updated_by,

  -- Judicial results
  judicial_results::text AS judicial_results_raw,

  created_at,
  created_by,
  updated_at,
  updated_by,
  is_deleted,
  version

FROM hmpps_court_case_service.offence
            WHERE legacy_id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "legacy_id" to rs.getLong("legacy_id"),
      "code" to rs.getString("code"),
      "title" to rs.getString("title"),
      "legislation" to rs.getString("legislation"),
      "listing_number" to rs.getInt("listing_number"),
      "sequence" to rs.getInt("sequence"),
      "short_term_custody_predictor_score" to rs.getInt("short_term_custody_predictor_score"),
      "wording" to rs.getString("wording"),

      // Verdict
      "legacy_verdict_id" to rs.getLong("legacy_verdict_id"),
      "verdict_date" to rs.getString("verdict_date"),
      "verdict_type" to rs.getString("verdict_type"),
      "verdict_version" to rs.getInt("verdict_version"),
      "verdict_created_at" to rs.getString("verdict_created_at"),
      "verdict_created_by" to rs.getString("verdict_created_by"),
      "verdict_is_deleted" to rs.getBoolean("verdict_is_deleted"),
      "verdict_last_updated_at" to rs.getString("verdict_last_updated_at"),
      "verdict_last_updated_by" to rs.getString("verdict_last_updated_by"),

      // Plea
      "legacy_plea_id" to rs.getLong("legacy_plea_id"),
      "plea_date" to rs.getString("plea_date"),
      "plea_value" to rs.getString("plea_value"),
      "plea_version" to rs.getInt("plea_version"),
      "plea_created_at" to rs.getString("plea_created_at"),
      "plea_created_by" to rs.getString("plea_created_by"),
      "plea_is_deleted" to rs.getBoolean("plea_is_deleted"),
      "plea_last_updated_at" to rs.getString("plea_last_updated_at"),
      "plea_last_updated_by" to rs.getString("plea_last_updated_by"),

      // Judicial results
      "judicial_results_raw" to rs.getString("judicial_results_raw"),

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
      val sourceValue = source[fieldSource]
      val targetValue = target[fieldTarget]
      if (sourceValue != targetValue) {
        errors.add("$label mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }

    // Offence
    compare("id", "legacy_id", "Offence ID")
    compare("offence_code", "code", "Offence code")
    compare("title", "title", "Title")
    compare("act", "legislation", "Act (or Legislation)")
    compare("list_no", "listing_number", "List number")
    compare("sequence", "sequence", "Sequence")
    compare("short_term_custody_predictor_score", "short_term_custody_predictor_score", "Custody predictor score")
    compare("summary", "wording", "Wording")

    // Plea
    compare("plea_id", "legacy_plea_id", "Plea ID")
    compare("plea_value", "plea_value", "Plea value")
    compare("plea_version", "plea_version", "Plea version")

    // Verdict
    compare("verdict_id", "legacy_verdict_id", "Verdict ID")
    compare("verdict_type_description", "verdict_type", "Verdict type")
    compare("verdict_version", "verdict_version", "Verdict version")

    // Judicial results
    errors += compareJudicialResults(
      source["judicial_results"]?.toString(),
      target["judicial_results_raw"]?.toString(),
      source["id"],
    )

    return errors
  }

  fun compareJudicialResults(sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    val fieldMappings = listOf(
      Triple("id", "legacyID", "ID"),
      Triple("is_convicted_result", "isConvictedResult", "Is convicted result"),
      Triple("judicial_result_type_id", "resultTypeID", "Judicial result type ID"),
      Triple("label", "label", "Label"),
      Triple("result_text", "resultText", "Result text"),
      Triple("created", "createdAt", "Created"),
      Triple("created_by", "createdBy", "Created by"),
      Triple("last_updated", "updatedAt", "Last updated"),
      Triple("last_updated_by", "updatedBy", "Last updated by"),
      Triple("deleted", "isDeleted", "Deleted"),
      Triple("version", "version", "Version"),
    )
    return compareJsonLists(sourceJson, targetJson, id, "Judicial Result", fieldMappings)
  }
}
