package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenceConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.offence"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.offence"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.offence o left join courtcaseservice.plea p on (o.plea_id = p.id) left join courtcaseservice.verdict v on (o.verdict_id = v.id)"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.offence o"

  const val SOURCE_QUERY = """        
          SELECT
          o.id, o.fk_hearing_defendant_id, o.offence_code, o.summary, o.title, o.sequence, o.act, o.list_no, 
          o.short_term_custody_predictor_score, o.created, o.created_by, o.last_updated, o.last_updated_by, 
          o.deleted, o.version,
      
          p.id AS plea_id, p.date AS plea_date, p.value AS plea_value, p.created AS plea_created, 
          p.created_by AS plea_created_by, p.last_updated AS plea_last_updated, 
          p.last_updated_by AS plea_last_updated_by, p.deleted AS plea_deleted, p.version AS plea_version,
      
          v.id AS verdict_id, v.date AS verdict_date, v.type_description AS verdict_type_description, 
          v.created AS verdict_created, v.created_by AS verdict_created_by, 
          v.last_updated AS verdict_last_updated, v.last_updated_by AS verdict_last_updated_by, 
          v.deleted AS verdict_deleted, v.version AS verdict_version,
      
          (
              SELECT json_agg(json_build_object(
                  'id', jr.id,
                  'is_convicted_result', jr.is_convicted_result,
                  'judicial_result_type_id', jr.judicial_result_type_id,
                  'label', jr.label,
                  'result_text', jr.result_text,
                  'created', jr.created,
                  'created_by', jr.created_by,
                  'last_updated', jr.last_updated,
                  'last_updated_by', jr.last_updated_by,
                  'deleted', jr.deleted,
                  'version', jr.version
              ))
              FROM courtcaseservice.judicial_result jr
              WHERE jr.offence_id = o.id
          ) AS judicial_results
      
      FROM courtcaseservice.offence o
      LEFT JOIN courtcaseservice.plea p ON o.plea_id = p.id
      LEFT JOIN courtcaseservice.verdict v ON o.verdict_id = v.id"""
}
