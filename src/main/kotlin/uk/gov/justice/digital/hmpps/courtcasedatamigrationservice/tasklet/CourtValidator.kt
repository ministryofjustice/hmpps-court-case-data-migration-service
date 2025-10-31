package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.jdbc.core.JdbcTemplate
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CourtConstants.SOURCE_QUERY

class CourtValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.court WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    "$SOURCE_QUERY WHERE c.id = ?",
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      // Court
      "id" to rs.getLong("id"),
      "name" to rs.getString("name"),
      "court_code" to rs.getString("court_code"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "deleted" to rs.getBoolean("deleted"),
      "version" to rs.getInt("version"),

      // Court rooms
      "court_rooms" to rs.getString("court_rooms"),

    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
        select 
        id,
        code,
        name,
        court_rooms::text as court_rooms_raw,
        created_at,
        created_by,
        updated_at,
        updated_by,
        is_deleted,
        version
        from hmpps_court_case_service.court_centre
            WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "code" to rs.getString("code"),
      "name" to rs.getString("name"),
      "court_rooms_raw" to rs.getString("court_rooms_raw"),
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

    compare("name", "name", "Name")
    compare("court_code", "code", "Code")

    errors += compareCourtRooms(
      errors,
      source["court_rooms"]?.toString(),
      target["court_rooms_raw"]?.toString(),
      source["id"],
    )

    return errors
  }

  fun compareCourtRooms(errors: MutableList<String>, sourceJson: String?, targetJson: String?, id: Any?): List<String> {
    if (sourceJson == null) {
      return errors
    }
    if (targetJson == null) {
      errors.add("Missing target court rooms for ID $id")
      return errors
    }

    val sourceList: List<Map<String, Any>> = objectMapper.readValue(sourceJson)
    val targetList: List<Map<String, Any?>> = objectMapper.readValue(targetJson)

    sourceList.sortedBy { it["court_room"].toString() }
    targetList.sortedBy { it["roomName"].toString() }

    for (i in sourceList.indices) {
      val sourceObject = sourceList[i]
      val targetObject = targetList[i]

      val sourceValue = sourceObject["court_room"]
      val targetValue = targetObject["roomName"]
      if (sourceValue != targetValue) {
        errors.add("Court Room mismatch for ID $id: '$sourceValue' vs '$targetValue'")
      }
    }
    return errors
  }
}
