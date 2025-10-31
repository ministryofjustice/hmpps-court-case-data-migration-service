package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenderConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.offender"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.offender"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.offender"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.offender"

  const val SOURCE_QUERY = """        
      select 
      o.id,
      o.suspended_sentence_order,
      o.breach,
      o.awaiting_psr,
      o.probation_status,
      o.pre_sentence_activity,
      o.previously_known_termination_date,
      o.created,
      o.created_by,
      o.last_updated,
      o.last_updated_by,
      o.deleted,
      o.version
      from courtcaseservice.offender o"""
}
