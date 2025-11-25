package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenderMatchConstants {

  // TODO this should be reviewed with Samuel for correctness. This might be creating duplicates in our new offender_match table.
  const val BASE_FROM_CLAUSE = """
        FROM courtcaseservice.offender_match om
        JOIN courtcaseservice.offender_match_group omg ON (om.group_id = omg.id)
        JOIN courtcaseservice.defendant d ON (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
        JOIN courtcaseservice.offender o ON (d.fk_offender_id = o.id)
        """

  const val MIN_QUERY = "SELECT MIN(om.id) $BASE_FROM_CLAUSE"
  const val MAX_QUERY = "SELECT MAX(om.id) $BASE_FROM_CLAUSE"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) $BASE_FROM_CLAUSE"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.offender_match om"

  const val SOURCE_QUERY = """
          SELECT 
              om.id,
              om.group_id,
              d.fk_offender_id,
              om.match_type,
              om.aliases,
              om.rejected,
              om.match_probability,
              om.created,
              om.last_updated,
              om.created_by,
              om.last_updated_by,
              om.deleted,
              om.version
          $BASE_FROM_CLAUSE"""

  const val SYNC_OFFENDER_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.offender"
  const val SYNC_OFFENDER_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.offender"

  const val SYNC_OFFENDER_ID_QUERY = """        
      SELECT
      o.id,
      o.legacy_id
     FROM
        hmpps_court_case_service.offender o"""

  const val SYNC_OFFENDER_MATCH_GROUP_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.offender_match_group"
  const val SYNC_OFFENDER_MATCH_GROUP_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.offender_match_group"

  const val SYNC_OFFENDER_MATCH_GROUP_ID_QUERY = """        
      SELECT
      omg.id,
      omg.legacy_id
     FROM
        hmpps_court_case_service.offender_match_group omg"""
}
