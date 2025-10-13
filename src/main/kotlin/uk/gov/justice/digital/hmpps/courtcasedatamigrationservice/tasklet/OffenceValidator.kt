package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.jdbc.core.JdbcTemplate

class OffenceValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.offence WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """
    SELECT
      o.id, o.fk_hearing_defendant_id, o.offence_code, o.summary, o.title, o.sequence, o.act, o.list_no, 
      o.short_term_custody_predictor_score, o.created, o.created_by, o.last_updated, o.last_updated_by, 
      o.deleted, o.version,

      p.id AS plea_id, p.date AS plea_date, p.value AS plea_value, p.created AS plea_created, 
      p.created_by AS plea_created_by, p.last_updated AS plea_last_updated, 
      p.last_updated_by AS plea_last_updated_by, p.deleted AS plea_deleted, p.version AS plea_version,

      v.id AS verdict_id, v.date AS verdict_date, v.type_description AS verdict_type_description, 
      v.created AS verdict_created, v.created_by AS verdict_created_by, 
      v.last_updated AS verdict_last_updated, v.last_updated_by AS verdict_last_updated_by, 
      v.deleted AS verdict_deleted, v.version AS verdict_version,

      (
          SELECT json_agg(json_build_object(
              'id', jr.id,
              'is_convicted_result', jr.is_convicted_result,
              'judicial_result_type_id', jr.judicial_result_type_id,
              'label', jr.label,
              'result_text', jr.result_text,
              'created', jr.created,
              'created_by', jr.created_by,
              'last_updated', jr.last_updated,
              'last_updated_by', jr.last_updated_by,
              'deleted', jr.deleted,
              'version', jr.version
          ))
          FROM courtcaseservice.judicial_result jr
          WHERE jr.offence_id = o.id
      ) AS judicial_results

    FROM courtcaseservice.offence o
    LEFT JOIN courtcaseservice.plea p ON o.plea_id = p.id
    LEFT JOIN courtcaseservice.verdict v ON o.verdict_id = v.id
    WHERE o.id = ?
    """.trimIndent(),
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
  id, 
  code, 
  title, 
  legislation,
  listing_number,
  sequence,
  short_term_custody_predictor_score,
  wording,
  
  -- Verdict
  verdict ->> 'id' AS verdict_id,
  verdict ->> 'date' AS verdict_date,
  verdict ->> 'type' AS verdict_type,
  verdict ->> 'version' AS verdict_version,
  verdict ->> 'createdAt' AS verdict_created_at,
  verdict ->> 'createdBy' AS verdict_created_by,
  verdict ->> 'isDeleted' AS verdict_is_deleted,
  verdict ->> 'lastUpdatedAt' AS verdict_last_updated_at,
  verdict ->> 'lastUpdatedBy' AS verdict_last_updated_by,

  -- Plea
  plea ->> 'id' AS plea_id,
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
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "code" to rs.getString("code"),
      "title" to rs.getString("title"),
      "legislation" to rs.getString("legislation"),
      "listing_number" to rs.getInt("listing_number"),
      "sequence" to rs.getInt("sequence"),
      "short_term_custody_predictor_score" to rs.getInt("short_term_custody_predictor_score"),
      "wording" to rs.getString("wording"),

      // Verdict
      "verdict_id" to rs.getLong("verdict_id"),
      "verdict_date" to rs.getString("verdict_date"),
      "verdict_type" to rs.getString("verdict_type"),
      "verdict_version" to rs.getInt("verdict_version"),
      "verdict_created_at" to rs.getString("verdict_created_at"),
      "verdict_created_by" to rs.getString("verdict_created_by"),
      "verdict_is_deleted" to rs.getBoolean("verdict_is_deleted"),
      "verdict_last_updated_at" to rs.getString("verdict_last_updated_at"),
      "verdict_last_updated_by" to rs.getString("verdict_last_updated_by"),

      // Plea
      "plea_id" to rs.getLong("plea_id"),
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

    // Basic fields
    compare("offence_code", "code", "Offence code")
    compare("title", "title", "Title")
    compare("act", "legislation", "Act (or Legislation)")
    compare("list_no", "listing_number", "List number")
    compare("sequence", "sequence", "Sequence")
    compare("short_term_custody_predictor_score", "short_term_custody_predictor_score", "Custody predictor score")

    // Plea
    compare("plea_id", "plea_id", "Plea ID")
    compare("plea_value", "plea_value", "Plea value")
    compare("plea_version", "plea_version", "Plea version")

    // Verdict
    compare("verdict_id", "verdict_id", "Verdict ID")
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

  val objectMapper = jacksonObjectMapper()

  fun compareJudicialResults(sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    val errors = mutableListOf<String>()

    val sourceList: List<Map<String, Any?>> = try {
      objectMapper.readValue(sourceJson ?: "[]")
    } catch (e: Exception) {
      errors.add("Failed to parse source judicial results for ID $id")
      return errors
    }

    val targetList: List<Map<String, Any?>> = try {
      objectMapper.readValue(targetJson ?: "[]")
    } catch (e: Exception) {
      errors.add("Failed to parse target judicial results for ID $id")
      return errors
    }

    if (sourceList.size != targetList.size) {
      errors.add("Judicial results count mismatch for ID $id: ${sourceList.size} vs ${targetList.size}")
    }

    val sortedSource = sourceList.sortedBy { it["id"].toString() }
    val sortedTarget = targetList.sortedBy { it["id"].toString() }

    fun compare(
      source: Map<String, Any?>,
      target: Map<String, Any?>,
      sourceField: String,
      targetField: String,
      label: String = sourceField,
      index: Int,
    ) {
      val sourceValue = source[sourceField]
      val targetValue = target[targetField]
      if (sourceValue != targetValue) {
        errors.add("Judicial result field '$label' mismatch for ID $id (index $index): '$sourceValue' vs '$targetValue'")
      }
    }

    val fieldMappings = listOf(
      Triple("id", "id", "ID"),
      Triple("is_convicted_result", "isConvictedResult", "Is convicted result"),
      Triple("judicial_result_type_id", "resultTypeId", "Judicial result type ID"),
      Triple("label", "label", "Label"),
      Triple("result_text", "resultText", "Result text"),
      Triple("created", "createdAt", "Created"),
      Triple("created_by", "createdBy", "Created by"),
      Triple("last_updated", "updatedAt", "Last updated"),
      Triple("last_updated_by", "updatedBy", "Last updated by"),
      Triple("deleted", "isDeleted", "Deleted"),
      Triple("version", "version", "Version"),
    )

    for (i in sortedSource.indices) {
      val sourceResult = sortedSource.getOrNull(i)
      val targetResult = sortedTarget.getOrNull(i)

      if (sourceResult == null || targetResult == null) continue

      for ((sourceField, targetField, label) in fieldMappings) {
        compare(sourceResult, targetResult, sourceField, targetField, label, i)
      }
    }

    return errors
  }
}
