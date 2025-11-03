package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object CourtConstants {

  const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.court"
  const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.court"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) from courtcaseservice.court"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.court_centre"

  const val SOURCE_QUERY = """        
    select 
		c.id, 
		c.name, 
		c.court_code,
		c.created,
		c.created_by,
		c.last_updated,
		c.last_updated_by,
		c.version,
		c.deleted,
          (
              SELECT json_agg(json_build_object(
                  'id', hd.id,
                  'court_room', hd.court_room
              ))
              FROM courtcaseservice.hearing_day hd
              WHERE hd.court_code = c.court_code
          ) AS court_rooms
	from courtcaseservice.court c"""
}
