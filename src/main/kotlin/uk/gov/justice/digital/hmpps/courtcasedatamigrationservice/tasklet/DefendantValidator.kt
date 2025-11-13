package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.springframework.jdbc.core.JdbcTemplate

class DefendantValidator(
  private val sourceJdbcTemplate: JdbcTemplate,
  private val targetJdbcTemplate: JdbcTemplate,
) : Validator() {

  override fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long> = sourceJdbcTemplate.queryForList(
    "SELECT id FROM courtcaseservice.defendant WHERE id BETWEEN ? AND ? ORDER BY RANDOM() LIMIT ?",
    arrayOf(minId, maxId, sampleSize),
    Long::class.java,
  )

  override fun fetchSourceRecord(id: Long): Map<String, Any>? = sourceJdbcTemplate.query(
    """
        SELECT 
        id, 
        defendant_name,
        type,
        name ->> 'forename1' AS forename1,
        name ->> 'forename2' AS forename2,
        name ->> 'surname' AS surname, 
        address ->> 'line1' as line1,
        address ->> 'line2' as line2,
        address ->> 'line3' as line3,
        address ->> 'line4' as line4,
        address ->> 'line5' as line5,
        address ->> 'postcode' as postcode,
        crn,
        pnc,
        cro,
        date_of_birth,
        sex,
        nationality_1,
        nationality_2,
        created,
        created_by,
        manual_update,
        defendant_id,
        offender_confirmed,
        phone_number ->> 'home' as home,
        phone_number ->> 'mobile' as mobile,
        person_id,
        fk_offender_id,
        last_updated,
        last_updated_by,
        version,
        deleted,
        tsv_name,
        cpr_uuid,
        c_id,
        fk_offender_id
        FROM courtcaseservice.defendant
        WHERE id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
      "id" to rs.getLong("id"),
      "defendant_name" to rs.getString("defendant_name"),
      "type" to rs.getString("type"),
      "forename1" to rs.getString("forename1"),
      "forename2" to rs.getString("forename2"),
      "surname" to rs.getString("surname"),
      "line1" to rs.getString("line1"),
      "line2" to rs.getString("line2"),
      "line3" to rs.getString("line3"),
      "line4" to rs.getString("line4"),
      "line5" to rs.getString("line5"),
      "postcode" to rs.getString("postcode"),
      "crn" to rs.getString("crn"),
      "pnc" to rs.getString("pnc"),
      "cro" to rs.getString("cro"),
      "date_of_birth" to rs.getDate("date_of_birth"),
      "sex" to rs.getString("sex"),
      "nationality_1" to rs.getString("nationality_1"),
      "nationality_2" to rs.getString("nationality_2"),
      "created" to rs.getTimestamp("created"),
      "created_by" to rs.getString("created_by"),
      "manual_update" to rs.getBoolean("manual_update"),
      "defendant_id" to rs.getString("defendant_id"),
      "offender_confirmed" to rs.getBoolean("offender_confirmed"),
      "home" to rs.getString("home"),
      "mobile" to rs.getString("mobile"),
      "person_id" to rs.getString("person_id"),
      "fk_offender_id" to rs.getLong("fk_offender_id"),
      "last_updated" to rs.getTimestamp("last_updated"),
      "last_updated_by" to rs.getString("last_updated_by"),
      "version" to rs.getInt("version"),
      "deleted" to rs.getBoolean("deleted"),
      "tsv_name" to rs.getString("tsv_name"),
      "cpr_uuid" to rs.getString("cpr_uuid"),
      "c_id" to rs.getLong("c_id"),
    )
  }.firstOrNull()

  override fun fetchTargetRecord(id: Long): Map<String, Any>? = targetJdbcTemplate.query(
    """
    SELECT
        id, 
        is_manual_update,
        crn,
        cro_number,
        tsv_name,
        pnc_id,
        cpr_uuid,
        is_offender_confirmed,
        person ->> 'firstName' AS firstName, 
        person ->> 'middleName' AS middleName,
        person ->> 'lastName' AS lastName,
        person ->> 'title' AS title,
        person -> 'sex' ->> 'code' AS sex,
        person ->> 'dateOfBirth' AS dateOfBirth,
        person ->> 'nationalityDescription' AS nationalityDescription,
        person ->> 'additionalNationalityDescription' AS additionalNationalityDescription,
        person ->> 'nationalityCode' AS nationalityCode,
        person ->> 'disabilityStatus' AS disabilityStatus,
        person -> 'address' ->> 'address1' AS address1,
        person -> 'address' ->> 'address2' AS address2,
        person -> 'address' ->> 'address3' AS address3,
        person -> 'address' ->> 'address4' AS address4,
        person -> 'address' ->> 'address5' AS address5,
        person -> 'address' ->> 'postcode' AS postcode,
        person -> 'contactInformation' ->> 'homeNumber' AS homeNumber,
        person -> 'contactInformation' ->> 'workNumber' AS workNumber,
        person -> 'contactInformation' ->> 'mobileNumber' AS mobileNumber,
        person -> 'contactInformation' ->> 'primaryEmail' AS primaryEmail,
        person -> 'contactInformation' ->> 'secondaryEmail' AS secondaryEmail,
        offender_id,
        created_at,
        created_by,
        updated_at,
        updated_by,
        is_deleted,
        version
    FROM hmpps_court_case_service.defendant
    WHERE legacy_id = ?
    """.trimIndent(),
    arrayOf(id),
  ) { rs, _ ->
    mapOf(
//      "id" to rs.getLong("id"), // TODO fix this
      "is_manual_update" to rs.getBoolean("is_manual_update"),
      "crn" to rs.getString("crn"),
      "cro_number" to rs.getString("cro_number"),
      "tsv_name" to rs.getString("tsv_name"),
      "pnc_id" to rs.getString("pnc_id"),
      "cpr_uuid" to rs.getString("cpr_uuid"),
      "is_offender_confirmed" to rs.getBoolean("is_offender_confirmed"),
      "firstName" to rs.getString("firstName"),
      "middleName" to rs.getString("middleName"),
      "lastName" to rs.getString("lastName"),
      "title" to rs.getString("title"),
      "sex" to rs.getString("sex"),
      "dateOfBirth" to rs.getString("dateOfBirth"),
      "nationalityDescription" to rs.getString("nationalityDescription"),
      "additionalNationalityDescription" to rs.getString("additionalNationalityDescription"),
      "nationalityCode" to rs.getString("nationalityCode"),
      "disabilityStatus" to rs.getString("disabilityStatus"),
      "address1" to rs.getString("address1"),
      "address2" to rs.getString("address2"),
      "address3" to rs.getString("address3"),
      "address4" to rs.getString("address4"),
      "address5" to rs.getString("address5"),
      "postcode" to rs.getString("postcode"),
      "homeNumber" to rs.getString("homeNumber"),
      "workNumber" to rs.getString("workNumber"),
      "mobileNumber" to rs.getString("mobileNumber"),
      "primaryEmail" to rs.getString("primaryEmail"),
      "secondaryEmail" to rs.getString("secondaryEmail"),
      "offender_id" to rs.getLong("offender_id"),
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

    compare("forename1", "firstName", "First name")
    compare("forename2", "middleName", "Middle name")
    compare("surname", "lastName", "Last name")
    compare("crn", "crn", "CRN")
    compare("cro", "cro_number", "CRO")
    compare("pnc", "pnc_id", "PNC")
    compare("cpr_uuid", "cpr_uuid", "CPR UUID")
//    compare("tsv_name", "tsv_name", "TSV name")
    compare("manual_update", "is_manual_update", "Manual update")
    compare("offender_confirmed", "is_offender_confirmed", "Offender confirmed")
    compare("sex", "sex", "Sex")
//    compare("date_of_birth", "dateOfBirth", "Date of birth")
    compare("nationality_1", "nationalityDescription", "Nationality 1")
    compare("nationality_2", "additionalNationalityDescription", "Nationality 2")
    compare("line1", "address1", "Address line 1")
    compare("line2", "address2", "Address line 2")
    compare("line3", "address3", "Address line 3")
    compare("line4", "address4", "Address line 4")
    compare("line5", "address5", "Address line 5")
    compare("postcode", "postcode", "Postcode")
    compare("home", "homeNumber", "Home number")
    compare("mobile", "mobileNumber", "Mobile number")
    compare("fk_offender_id", "offender_id", "Offender ID")
    compare("created", "created_at", "Created At")
    compare("created_by", "created_by", "Created By")
    compare("last_updated", "updated_at", "Updated At")
    compare("last_updated_by", "updated_by", "Updated By")
    compare("version", "version", "Version")
    compare("deleted", "is_deleted", "Deleted")

    return errors
  }
}
