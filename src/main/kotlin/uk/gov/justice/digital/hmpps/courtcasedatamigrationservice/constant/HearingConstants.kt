package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object HearingConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.hearing"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.hearing"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.hearing"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.hearing"

  const val SOURCE_QUERY = """        
    SELECT
    h.id,
    h.hearing_type,
    h.hearing_event_type,
    h.list_no,
    h.first_created,
    h.created, 
    h.created_by, 
    h.last_updated, 
    h.last_updated_by, 
    h.deleted, 
    h.version,
    (
        SELECT json_agg(json_build_object(
            'id', ho.id,
            'defendant_id', hd.defendant_id,
            'outcome_type', ho.outcome_type,
            'outcome_date', ho.outcome_date,
            'state', ho.state,
            'assigned_to', ho.assigned_to,
            'assigned_to_uuid', ho.assigned_to_uuid,
            'resulted_date', ho.resulted_date,
            'legacy', ho.legacy,
            'created', ho.created, 
            'created_by', ho.created_by, 
            'last_updated', ho.last_updated, 
            'last_updated_by', ho.last_updated_by, 
            'deleted', ho.deleted, 
            'version', ho.version
        ))
        FROM courtcaseservice.hearing_outcome ho
        JOIN courtcaseservice.hearing_defendant hd 
            ON ho.fk_hearing_defendant_id = hd.id
        WHERE hd.fk_hearing_id = h.id
    ) AS hearing_outcomes,
    (
        SELECT json_agg(json_build_object(
            'id', hn.id,
            'defendant_id', hd.defendant_id,
            'note', hn.note,
            'author', hn.author,
            'draft', hn.draft,
            'legacy', hn.legacy,
            'created_by_uuid', hn.created_by_uuid,
            'created', hn.created, 
            'created_by', hn.created_by, 
            'last_updated', hn.last_updated, 
            'last_updated_by', hn.last_updated_by, 
            'deleted', hn.deleted, 
            'version', hn.version
        ))
        FROM courtcaseservice.hearing_notes hn
        JOIN courtcaseservice.hearing_defendant hd 
            ON hn.fk_hearing_defendant_id = hd.id
        WHERE hd.fk_hearing_id = h.id
    ) AS hearing_notes
	FROM courtcaseservice.hearing h"""
}
