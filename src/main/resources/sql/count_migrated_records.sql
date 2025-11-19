SELECT
    (SELECT COUNT(*) FROM hmpps_court_case_service.offence) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.offender) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.defendant) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.hearing) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.prosecution_case) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.court_centre) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.defendant_offence) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.offender_match_group) +
    (SELECT COUNT(*) FROM hmpps_court_case_service.offender_match)
        AS total_count;