package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object CaseConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.court_case"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.court_case"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.court_case"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.prosecution_case"

  const val SOURCE_QUERY = """        
    SELECT
    cc.id,
    cc.urn,
    cc.source_type,
    cc.created,
    cc.created_by,
    cc.last_updated,
    cc.last_updated_by,
    cc.deleted,
    cc.version,
    (
        SELECT json_agg(json_build_object(
            'id', cdd.id,
            'document_id', cdd.document_id,
            'document_name', cdd.document_name,
            'created', cdd.created,
            'created_by', cdd.created_by,
            'last_updated', cdd.last_updated,
            'last_updated_by', cdd.last_updated_by,
            'deleted', cdd.deleted,
            'version', cdd.version
        ))
        FROM courtcaseservice.case_defendant cd
        JOIN courtcaseservice.case_defendant_documents cdd 
            ON cdd.fk_case_defendant_id = cd.id
        WHERE cd.fk_court_case_id = cc.id
    ) AS case_documents,
    (
        SELECT json_agg(json_build_object(
            'id', cm.id,
            'type_description', cm.type_description,
            'created', cm.created,
            'created_by', cm.created_by,
            'last_updated', cm.last_updated,
            'last_updated_by', cm.last_updated_by,
            'deleted', cm.deleted,
            'version', cm.version
        ))
        FROM courtcaseservice.case_marker cm
        WHERE cm.fk_court_case_id = cc.id
    ) AS case_markers

      FROM
          courtcaseservice.court_case cc"""
}
