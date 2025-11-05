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
}
