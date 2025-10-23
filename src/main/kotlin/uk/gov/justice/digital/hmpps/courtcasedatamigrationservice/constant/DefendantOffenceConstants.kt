package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object DefendantOffenceConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.hearing"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.hearing"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.hearing"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.hearing"

  //  select
// 	hd.id,
//    d.id AS defendant_id,
//    o.id AS offence_id
// FROM
//    courtcaseservice.defendant d
// JOIN
//    courtcaseservice.hearing_defendant hd ON hd.fk_defendant_id  = d.id
// JOIN
//    courtcaseservice.offence o ON o.fk_hearing_defendant_id  = hd.id;

  const val SOURCE_QUERY = """        
    select
        hd.id,
        o.id as offence_id,
        hd.fk_defendant_id as defendant_id,
        hd.created,
        hd.created_by,
        hd.last_updated,
        hd.last_updated_by,
        hd.deleted,
        hd.version
        from courtcaseservice.offence o join courtcaseservice.hearing_defendant hd on hd.id = o.fk_hearing_defendant_id"""
}
