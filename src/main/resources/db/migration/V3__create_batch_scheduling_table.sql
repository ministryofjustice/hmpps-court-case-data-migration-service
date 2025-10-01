CREATE TABLE hmpps_court_case_batch_metadata.HMPPS_BATCH_SCHEDULING_CONFIG (
                                                                    ID INT4 NOT NULL PRIMARY KEY,
                                                                    JOB_NAME VARCHAR(100) NOT NULL,
                                                                    IS_ENABLED BOOL NOT NULL
);

INSERT INTO hmpps_court_case_batch_metadata.HMPPS_BATCH_SCHEDULING_CONFIG (ID, JOB_NAME, IS_ENABLED) VALUES (1, 'OFFENCE', FALSE);