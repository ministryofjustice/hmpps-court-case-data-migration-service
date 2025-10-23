package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object DefendantConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.defendant"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.defendant"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM courtcaseservice.defendant d"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.defendant d"

  const val SOURCE_QUERY = """        
    SELECT d.id, 
    d.manual_update, 
    d.crn, 
    d.cro, 
    d.name, 
    d.date_of_birth, 
    d.offender_confirmed, 
    d.nationality_1, 
    d.nationality_2, 
    d.sex, 
    d.phone_number, 
    d.address, 
    d.tsv_name, 
    d.pnc,
    d.cpr_uuid, 
    d.created, 
    d.created_by, 
    d.last_updated, 
    d.last_updated_by, 
    d.deleted, 
    d.version
FROM courtcaseservice.defendant d"""
}
