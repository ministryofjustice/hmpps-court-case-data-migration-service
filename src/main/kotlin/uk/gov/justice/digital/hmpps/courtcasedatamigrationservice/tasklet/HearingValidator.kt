package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.HearingConstants.SOURCE_QUERY

class HearingValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.hearing WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE h.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "hearing_type" to rs.getString("hearing_type"),
      "hearing_event_type" to rs.getString("hearing_event_type"),
      "list_no" to rs.getString("list_no"),
      "first_created" to rs.getTimestamp("first_created"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "deleted" to rs.getBoolean("deleted"),
      "version" to rs.getInt("version"),

      "hearing_outcomes" to rs.getString("hearing_outcomes"),
      "hearing_notes" to rs.getString("hearing_notes"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
            select 
            legacy_id, 
            type, 
            event_type, 
            list_number, 
            first_created, 
            
            -- Hearing Outcomes
              hearing_outcome::text AS hearing_outcomes_raw, 
              
            -- Hearing Case Notes
              hearing_case_note::text as hearing_case_notes_raw,
            
              created_at,
              created_by,
              updated_at,
              updated_by,
              is_deleted,
              version
            
            from hmpps_court_case_service.hearing
            where legacy_id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "legacy_id" to rs.getLong("legacy_id"),
      "type" to rs.getString("type"),
      "event_type" to rs.getString("event_type"),
      "list_number" to rs.getString("list_number"),
      "first_created" to rs.getTimestamp("first_created"),

      // Hearing Outcomes
      "hearing_outcomes_raw" to rs.getString("hearing_outcomes_raw"),

      // Hearing Case Notes
      "hearing_case_notes_raw" to rs.getString("hearing_case_notes_raw"),

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

    fun compare(fieldSource: String, fieldTarget: String = fieldSource, label: String = fieldSource) {
      val sourceValue = source[fieldSource]
      val targetValue = target[fieldTarget]
      if (sourceValue != targetValue) {
        errors.add("$label mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }

    compare("id", "legacy_id", "Hearing ID")
    compare("hearing_type", "type", "Hearing type")
    compare("hearing_event_type", "event_type", "Hearing event type")
    compare("list_no", "list_number", "List number")
    compare("first_created", "first_created", "First created")
    compare("created", "created_at", "Created")
    compare("created_by", "created_by", "Created by")
    compare("last_updated", "updated_at", "Last updated")
    compare("last_updated_by", "updated_by", "Last updated by")
    compare("deleted", "is_deleted", "Deleted")
    compare("version", "version", "Version")

    // Hearing outcomes
    errors += compareHearingOutcomes(
      source["hearing_outcomes"]?.toString(),
      target["hearing_outcomes_raw"]?.toString(),
      source["id"],
    )

    // Hearing case notes
    errors += compareHearingCaseNotes(
      source["hearing_notes"]?.toString(),
      target["hearing_case_notes_raw"]?.toString(),
      source["id"],
    )

    return errors
  }

  fun compareHearingOutcomes(sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    val fieldMappings = listOf(
      Triple("id", "legacyID", "Hearing Outcomes ID"),
      Triple("defendant_id", "defendantID", "Defendant ID"),
      Triple("outcome_type", "type", "Type"),
      Triple("state", "state", "State"),
      Triple("legacy", "isLegacy", "Is legacy"),
      Triple("assigned_to", "assignedTo", "Assigned to"),
      Triple("assigned_to_uuid", "assignedToUUID", "Assigned to UUID"),
      Triple("resulted_date", "resultedDate", "Resulted date"),
      Triple("created_by", "createdBy", "Created by"),
      Triple("last_updated", "updatedAt", "Last updated"),
      Triple("last_updated_by", "updatedBy", "Last updated by"),
      Triple("deleted", "isDeleted", "Deleted"),
      Triple("version", "version", "Version"),
    )
    return compareJsonLists(sourceJson, targetJson, id, "Hearing outcome", fieldMappings)
  }

  fun compareHearingCaseNotes(sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    val fieldMappings = listOf(
      Triple("id", "legacyID", "Hearing Case Notes ID"),
      Triple("defendant_id", "defendantID", "Defendant ID"),
      Triple("note", "note", "Note"),
      Triple("author", "author", "Author"),
      Triple("legacy", "isLegacy", "Is legacy"),
      Triple("draft", "isDraft", "Draft"),
      Triple("created_by_uuid", "createdByUUID", "Created by UUID"),
      Triple("resulted_date", "resultedDate", "Resulted date"),
      Triple("created_by", "createdBy", "Created by"),
      Triple("last_updated_by", "updatedBy", "Last updated by"),
      Triple("deleted", "isDeleted", "Deleted"),
      Triple("version", "version", "Version"),
    )
    return compareJsonLists(sourceJson, targetJson, id, "Hearing case notes", fieldMappings)
  }
}
