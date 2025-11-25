package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant

object OffenderMatchGroupConstants {

  /**
   *
   * TODO use this query to see the issue with 1-many in the court_case join. Solution attempted in the queries below but need to be reviewed.
   *     	select count(*), id	from (
   * 		select
   *         omg.id,
   *         d.id defendant_id,
   *         cc.id case_id,
   *         omg.created,
   *         omg.last_updated,
   *         omg.created_by,
   *         omg.last_updated_by,
   *         omg.deleted,
   *         omg.version
   *         from courtcaseservice.offender_match_group omg 	join courtcaseservice.court_case cc on (omg.case_id = cc.case_id)
   *                                     join courtcaseservice.defendant d on (nullif(omg.defendant_id, 'null')::uuid = d.defendant_id)) tab_a group by id
   *                                     having count(*) > 1;
   *
   *
   */

  // TODO this should be reviewed with Samuel for correctness - records filtered out.
  const val BASE_FROM_CLAUSE = """
        FROM courtcaseservice.offender_match_group omg
        JOIN LATERAL (
            SELECT *
            FROM courtcaseservice.court_case cc
            WHERE cc.case_id = omg.case_id
            ORDER BY cc.last_updated DESC NULLS LAST
            LIMIT 1
        ) cc ON true
        JOIN courtcaseservice.defendant d 
            ON (NULLIF(omg.defendant_id, 'null')::uuid = d.defendant_id)
        """

  const val MIN_QUERY = "SELECT MIN(omg.id) $BASE_FROM_CLAUSE"
  const val MAX_QUERY = "SELECT MAX(omg.id) $BASE_FROM_CLAUSE"
  const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) $BASE_FROM_CLAUSE"
  const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.offender_match_group"

  const val SOURCE_QUERY = """
          SELECT 
              omg.id,
              d.id AS defendant_id,
              cc.id AS case_id,
              omg.created,
              omg.last_updated,
              omg.created_by,
              omg.last_updated_by,
              omg.deleted,
              omg.version
          $BASE_FROM_CLAUSE
          """

  const val SYNC_DEFENDANT_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.defendant"
  const val SYNC_DEFENDANT_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.defendant"

  const val SYNC_DEFENDANT_ID_QUERY = """        
      SELECT
      d.id,
      d.legacy_id
     FROM
        hmpps_court_case_service.defendant d"""

  const val SYNC_PROSECUTION_CASE_ID_MIN_QUERY = "SELECT MIN(legacy_id) FROM hmpps_court_case_service.defendant"
  const val SYNC_PROSECUTION_CASE_ID_MAX_QUERY = "SELECT MAX(legacy_id) FROM hmpps_court_case_service.defendant"

  const val SYNC_PROSECUTION_CASE_ID_QUERY = """        
      SELECT
      pc.id,
      pc.legacy_id
     FROM
        hmpps_court_case_service.prosecution_case pc"""
}
