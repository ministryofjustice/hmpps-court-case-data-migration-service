ALTER TABLE hmpps_court_case_service.defendant_offence
    ADD CONSTRAINT fk_defendant_offence_offence FOREIGN KEY (offence_id) REFERENCES hmpps_court_case_service.offence(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_defendant_offence_defendant FOREIGN KEY (defendant_id) REFERENCES hmpps_court_case_service.defendant(id) ON DELETE CASCADE;

ALTER TABLE hmpps_court_case_service.defendant
    ADD CONSTRAINT fk_defendant_offender FOREIGN KEY (offender_id) REFERENCES hmpps_court_case_service.offender(id) ON DELETE CASCADE;

ALTER TABLE hmpps_court_case_service.offender_match_group
    ADD CONSTRAINT fk_offender_match_group_defendant FOREIGN KEY (defendant_id) REFERENCES hmpps_court_case_service.defendant(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_offender_match_group_prosecution_case FOREIGN KEY (prosecution_case_id) REFERENCES hmpps_court_case_service.prosecution_case(id) ON DELETE CASCADE;

ALTER TABLE hmpps_court_case_service.offender_match
    ADD CONSTRAINT fk_offender_match_offender FOREIGN KEY (offender_id) REFERENCES hmpps_court_case_service.offender(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_offender_match_group FOREIGN KEY (offender_match_group_id) REFERENCES hmpps_court_case_service.offender_match_group(id) ON DELETE CASCADE;

ALTER TABLE hmpps_court_case_service.case_comment
    ADD CONSTRAINT fk_case_comment_defendant FOREIGN KEY (defendant_id) REFERENCES hmpps_court_case_service.defendant(id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_case_comment_case FOREIGN KEY (case_id) REFERENCES hmpps_court_case_service.prosecution_case(id) ON DELETE CASCADE;

-- ALTER TABLE hmpps_court_case_service.prosecution_case_hearing
--     ADD CONSTRAINT fk_prosecution_case_hearing_hearing FOREIGN KEY (hearing_id) REFERENCES hmpps_court_case_service.hearing(id) ON DELETE CASCADE,
--     ADD CONSTRAINT fk_prosecution_case_hearing_case FOREIGN KEY (prosecution_case_id) REFERENCES hmpps_court_case_service.prosecution_case(id) ON DELETE CASCADE;

-- ALTER TABLE hmpps_court_case_service.defendant_hearing
--     ADD CONSTRAINT fk_defendant_hearing_defendant FOREIGN KEY (defendant_id) REFERENCES hmpps_court_case_service.defendant(id) ON DELETE CASCADE,
--     ADD CONSTRAINT fk_defendant_hearing_hearing FOREIGN KEY (hearing_id) REFERENCES hmpps_court_case_service.hearing(id) ON DELETE CASCADE;

-- ALTER TABLE hmpps_court_case_service.hearing_day
--     ADD CONSTRAINT fk_hearing_day_hearing FOREIGN KEY (hearing_id) REFERENCES hmpps_court_case_service.hearing(id) ON DELETE CASCADE,
--     ADD CONSTRAINT fk_hearing_day_court_centre FOREIGN KEY (court_centre_id) REFERENCES hmpps_court_case_service.court_centre(id) ON DELETE CASCADE;

-- ALTER TABLE hmpps_court_case_service.defendant_prosecution_case
--     ADD CONSTRAINT fk_defendant_prosecution_case_defendant FOREIGN KEY (defendant_id) REFERENCES hmpps_court_case_service.defendant(id) ON DELETE CASCADE,
--     ADD CONSTRAINT fk_defendant_prosecution_case_case FOREIGN KEY (prosecution_case_id) REFERENCES hmpps_court_case_service.prosecution_case(id) ON DELETE CASCADE;
