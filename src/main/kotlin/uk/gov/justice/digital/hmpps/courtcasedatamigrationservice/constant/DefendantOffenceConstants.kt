package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object DefendantOffenceConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.hearing_defendant"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.hearing_defendant"
  const val SOURCE_ROW_COUNT_QUERY = """SELECT COUNT(*) FROM
        courtcaseservice.defendant d
     JOIN
        courtcaseservice.hearing_defendant hd ON hd.fk_defendant_id  = d.id
     JOIN
        courtcaseservice.offence o ON o.fk_hearing_defendant_id  = hd.id"""
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.defendant_offence"

  const val SOURCE_QUERY = """        
      SELECT
        hd.id,
        d.id AS legacy_defendant_id,
        o.id AS legacy_offence_id,
        hd.created, 
        hd.created_by, 
        hd.last_updated, 
        hd.last_updated_by, 
        hd.deleted, 
        hd.version
     FROM
        courtcaseservice.defendant d
     JOIN
        courtcaseservice.hearing_defendant hd ON hd.fk_defendant_id  = d.id
     JOIN
        courtcaseservice.offence o ON o.fk_hearing_defendant_id  = hd.id"""

  const val SYNC_DEFENDANT_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.defendant"
  const val SYNC_DEFENDANT_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.defendant"
  const val SYNC_DEFENDANT_ID_SOURCE_ROW_COUNT_QUERY = "select count(*) from courtcaseservice.defendant"
  const val SYNC_DEFENDANT_ID_TARGET_ROW_COUNT_QUERY = "select count(*) from hmpps_court_case_service.defendant_offence where defendant_id is not null" // TODO review this

  const val SYNC_DEFENDANT_ID_QUERY = """        
      SELECT
      d.id,
      d.defendant_id,
      d.legacy_id
     FROM
        hmpps_court_case_service.defendant d"""

  const val SYNC_OFFENCE_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.offence"
  const val SYNC_OFFENCE_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.offence"
  const val SYNC_OFFENCE_ID_SOURCE_ROW_COUNT_QUERY = "select count(*) from courtcaseservice.offence"
  const val SYNC_OFFENCE_ID_TARGET_ROW_COUNT_QUERY = "select count(*) from hmpps_court_case_service.defendant_offence where offence_id is not null" // TODO review this

  const val SYNC_OFFENCE_ID_QUERY = """        
      SELECT
      o.id,
      o.legacy_id
     FROM
        hmpps_court_case_service.offence o"""
}
