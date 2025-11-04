package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenderMatchGroupConstants {

  // TODO all of these queries need to bew reviewed because they will filter out omg records which are orphaned by case_id and defendant_id
  const val MIN_QUERY = """
    select min(omg.id)
    from courtcaseservice.offender_match_group omg 	join courtcaseservice.court_case cc on (omg.case_id = cc.case_id)
    												join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)"""
  const val MAX_QUERY = """
    select max(omg.id)
    from courtcaseservice.offender_match_group omg 	join courtcaseservice.court_case cc on (omg.case_id = cc.case_id)
    												join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)"""
  const val SOURCE_ROW_COUNT_QUERY = """
    select count(*)
    from courtcaseservice.offender_match_group omg 	join courtcaseservice.court_case cc on (omg.case_id = cc.case_id)
    												join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)"""
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.offender_match_group"

  const val SOURCE_QUERY = """        
	  select 
    omg.id,
    cc.id as case_id,
    d.id as defendant_id,
    omg.created,
    omg.last_updated,
    omg.created_by,
    omg.last_updated_by,
    omg.deleted,
    omg.version 
    from courtcaseservice.offender_match_group omg join courtcaseservice.court_case cc on (omg.case_id = cc.case_id)
    												join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)"""
}
