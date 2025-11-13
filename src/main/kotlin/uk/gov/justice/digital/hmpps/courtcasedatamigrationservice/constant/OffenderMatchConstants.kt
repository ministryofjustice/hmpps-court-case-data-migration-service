package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenderMatchConstants {

  // TODO all of these need to be reviewed by Sam. Also they should be refactored as theres a lot of duplication.
  const val MIN_QUERY = """SELECT MIN(om.id) from courtcaseservice.offender_match om join courtcaseservice.offender_match_group omg on (om.group_id = omg.id)
    										join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
    										join courtcaseservice.offender o on (d.fk_offender_id = o.id)"""
  const val MAX_QUERY = """SELECT MAX(om.id) from courtcaseservice.offender_match om join courtcaseservice.offender_match_group omg on (om.group_id = omg.id)
    										join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
    										join courtcaseservice.offender o on (d.fk_offender_id = o.id)"""
  const val SOURCE_ROW_COUNT_QUERY = """SELECT COUNT(*) from courtcaseservice.offender_match om join courtcaseservice.offender_match_group omg on (om.group_id = omg.id)
    										join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
    										join courtcaseservice.offender o on (d.fk_offender_id = o.id)"""
  const val TARGET_ROW_COUNT_QUERY = """SELECT COUNT(*) from hmpps_court_case_service.offender_match om"""

  // TODO this query needs to be reviewed by Sam
  const val SOURCE_QUERY = """        
    select 
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
    from courtcaseservice.offender_match om join courtcaseservice.offender_match_group omg on (om.group_id = omg.id)
    										join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)
    										join courtcaseservice.offender o on (d.fk_offender_id = o.id)"""

  const val SYNC_OFFENDER_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.offender"
  const val SYNC_OFFENDER_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.offender"
  const val SYNC_OFFENDER_ID_SOURCE_ROW_COUNT_QUERY = "select count(*) from courtcaseservice.defendant"
  const val SYNC_OFFENDER_ID_TARGET_ROW_COUNT_QUERY = "select count(*) from hmpps_court_case_service.defendant_offence where defendant_id is not null" // TODO review this

  const val SYNC_OFFENDER_ID_QUERY = """        
      SELECT
      o.id,
      o.legacy_id
     FROM
        hmpps_court_case_service.offender o"""

  const val SYNC_OFFENDER_MATCH_GROUP_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.offender_match_group"
  const val SYNC_OFFENDER_MATCH_GROUP_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.offender_match_group"
  const val SYNC_OFFENDER_MATCH_GROUP_ID_SOURCE_ROW_COUNT_QUERY = "select count(*) from courtcaseservice.offender_match_group"
  const val SYNC_OFFENDER_MATCH_GROUP_ID_TARGET_ROW_COUNT_QUERY = "select count(*) from hmpps_court_case_service.offender_match_group" // TODO review this

  const val SYNC_OFFENDER_MATCH_GROUP_ID_QUERY = """        
      SELECT
      omg.id,
      omg.legacy_id
     FROM
        hmpps_court_case_service.offender_match_group omg"""
}
