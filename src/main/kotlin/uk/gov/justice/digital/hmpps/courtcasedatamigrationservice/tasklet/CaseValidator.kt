package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseConstants.SOURCE_QUERY

class CaseValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.court_case WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE cc.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      // Case
      "id" to rs.getLong("id"),
      "urn" to rs.getString("urn"),
      "source_type" to rs.getString("source_type"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "deleted" to rs.getBoolean("deleted"),
      "version" to rs.getInt("version"),

      // Case Documents
      "case_documents" to rs.getString("case_documents"),

      // Case Markers
      "case_markers" to rs.getString("case_markers"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
        select 
        id,
        case_urn::text AS case_urns_raw,
        source_type,
        c_id,
        case_markers::text AS case_markers_raw,
        case_documents::text AS case_documents_raw,
        created_at,
        created_by,
        updated_at,
        updated_by,
        is_deleted,
        version
        from hmpps_court_case_service.prosecution_case
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "case_urns_raw" to rs.getString("case_urns_raw"),
      "source_type" to rs.getString("source_type"),
      "c_id" to rs.getString("c_id"),
      "case_markers_raw" to rs.getString("case_markers_raw"),
      "case_documents_raw" to rs.getString("case_documents_raw"),
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

    compareCaseURNs(source, target, errors, id)

    compare("source_type", "source_type", "Source Type")

    errors += compareCaseMarkers(
      source["case_markers"]?.toString(),
      target["case_markers_raw"]?.toString(),
      source["id"],
    )

    errors += compareCaseDocuments(
      source["case_documents"]?.toString(),
      target["case_documents_raw"]?.toString(),
      source["id"],
    )

    return errors
  }

  private fun compareCaseURNs(
    source: Map<String, Any>,
    target: Map<String, Any>,
    errors: MutableList<String>,
    id: Any?,
  ) {
    val caseURNSourceValue = source["urn"]
    val caseURNTargetValue = getCaseURN(target["case_urns_raw"]?.toString())
    if (caseURNSourceValue != caseURNTargetValue) {
      errors.add("Case URN mismatch for ID $id: '$caseURNSourceValue' vs '$caseURNTargetValue'")
    }
  }

  fun compareCaseMarkers(sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    val fieldMappings = listOf(
      Triple("id", "id", "ID"),
      Triple("type_description", "typeDescription", "Type Description"),
//      Triple("created", "createdAt", "Created"),
      Triple("created_by", "createdBy", "Created by"),
//      Triple("last_updated", "updatedAt", "Last updated"),
      Triple("last_updated_by", "updatedBy", "Last updated by"),
      Triple("deleted", "isDeleted", "Deleted"),
      Triple("version", "version", "Version"),
    )
    return compareJsonLists(sourceJson, targetJson, id, "Case Marker", fieldMappings)
  }

  fun compareCaseDocuments(sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    val fieldMappings = listOf(
      Triple("id", "id", "ID"),
      Triple("document_id", "documentId", "Document ID"),
      Triple("document_name", "documentName", "Document Name"),
//      Triple("created", "createdAt", "Created"),
      Triple("created_by", "createdBy", "Created by"),
//      Triple("last_updated", "updatedAt", "Last updated"),
      Triple("last_updated_by", "updatedBy", "Last updated by"),
      Triple("deleted", "isDeleted", "Deleted"),
      Triple("version", "version", "Version"),
    )
    return compareJsonLists(sourceJson, targetJson, id, "Case Document", fieldMappings)
  }

  fun getCaseURN(jsonString: String?): String? {
    return try {
      if (jsonString == null) {
        return null
      }
      val mapper = jacksonObjectMapper()
      val list: List<Map<String, Any>> = mapper.readValue(jsonString)
      list.firstOrNull()?.get("caseURN") as? String
    } catch (e: Exception) {
      e.printStackTrace()
      null
    }
  }
}
