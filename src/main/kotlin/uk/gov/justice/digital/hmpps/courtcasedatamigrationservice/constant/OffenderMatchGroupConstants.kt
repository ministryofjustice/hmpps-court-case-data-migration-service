package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenderMatchGroupConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.offender_match_group"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.offender_match_group"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.offender_match_group"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.offender_match_group"

  const val SOURCE_QUERY = """        
    SELECT 
    id,
    case_id,
    defendant_id,
    created,
    last_updated,
    created_by,
    last_updated_by,
    deleted,
    version
    FROM courtcaseservice.offender_match_group"""
}
