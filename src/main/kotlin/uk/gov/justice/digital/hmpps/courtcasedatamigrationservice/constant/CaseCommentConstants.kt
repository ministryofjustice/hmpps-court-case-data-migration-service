package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object CaseCommentConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.case_comments"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.case_comments"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.case_comments"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.case_comment"

  const val SOURCE_QUERY = """        
      select 
      cc.id,
      cc.defendant_id,
      cc.case_id,
      cc.author,
      cc.comment,
      cc.is_draft,
      cc.legacy,
      cc.created,
      cc.created_by,
      cc.last_updated,
      cc.last_updated_by,
      cc.deleted,
      cc.version
      from courtcaseservice.case_comments cc"""

  const val SYNC_DEFENDANT_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.defendant"
  const val SYNC_DEFENDANT_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.defendant"

  const val SYNC_DEFENDANT_ID_QUERY = """        
      SELECT
      d.id,
      d.defendant_id
     FROM
        hmpps_court_case_service.defendant d"""
}
