

---- SRC LAYER ----
WITH
SRC_PRVDR as ( SELECT *     from      STAGING.DST_PROVIDER ),
//SRC_PRVDR as ( SELECT *     from     DST_PROVIDER) ,

---- LOGIC LAYER ----

LOGIC_PRVDR as ( SELECT 
		  UNIQUE_ID_KEY                                      AS                                      UNIQUE_ID_KEY 
		, PEACH_NUMBER                                       AS                                       PEACH_NUMBER 
		, PEACH_FORMATTED_NUMBER                             AS                             PEACH_FORMATTED_NUMBER 
		, PEACH_BASE_NUMBER                                  AS                                  PEACH_BASE_NUMBER 
		, PEACH_SUFFIX_NUMBER                                AS                                PEACH_SUFFIX_NUMBER 
		, PRVDR_LEGAL_NAME                                   AS                                   PRVDR_LEGAL_NAME 
		, PRSN_IND                                           AS                                           PRSN_IND 
		, GNDR_CODE                                          AS                                          GNDR_CODE 
		, BIRTH_DATE                                         AS                                         BIRTH_DATE 
		, DEATH_DATE                                         AS                                         DEATH_DATE 
		, BSNS_TYPE_CODE                                     AS                                     BSNS_TYPE_CODE 
		, DBA_NAME                                           AS                                           DBA_NAME 
		, PRCT_TYPE_CODE                                     AS                                     PRCT_TYPE_CODE 
		, PRCT_TYPE_NAME                                     AS                                     PRCT_TYPE_NAME 
		, PRVDR_TYPE_CODE                                    AS                                    PRVDR_TYPE_CODE 
		, PRVDR_TYPE_DESC                                    AS                                    PRVDR_TYPE_DESC 
		, PROVIDER_SPECIALTY_LIST_TEXT                       as                       PROVIDER_SPECIALTY_LIST_TEXT 
		, PROVIDER_PHONE_NUMBER                              AS                              PROVIDER_PHONE_NUMBER 
		, PROVIDER_FAX_NUMBER                                AS                                PROVIDER_FAX_NUMBER 
		, PROVIDER_EMAIL_ADDRESS                             AS                             PROVIDER_EMAIL_ADDRESS 
		, OUT_OF_STATE_STATUS_DESC                           AS                           OUT_OF_STATE_STATUS_DESC 
		, PRACTICE_STREET_ADDRESS_1                          AS                          PRACTICE_STREET_ADDRESS_1 
		, PRACTICE_STREET_ADDRESS_2                          AS                          PRACTICE_STREET_ADDRESS_2 
		, PRACTICE_CITY_NAME                                 AS                                 PRACTICE_CITY_NAME 
		, PRACTICE_STATE_CODE                                AS                                PRACTICE_STATE_CODE 
		, PRACTICE_ZIP_CODE_NMBR                             AS                             PRACTICE_ZIP_CODE_NMBR 
		, PRACTICE_ZIP_CODE_PLS4_NMBR                        AS                        PRACTICE_ZIP_CODE_PLS4_NMBR 
		, PRACTICE_FULL_ZIP_CODE_NUMBER                      AS                      PRACTICE_FULL_ZIP_CODE_NUMBER 
		, PRACTICE_COUNTY_NAME                               AS                               PRACTICE_COUNTY_NAME 
		, CORRESPONDENCE_STREET_ADDRESS_1                    AS                    CORRESPONDENCE_STREET_ADDRESS_1 
		, CORRESPONDENCE_STREET_ADDRESS_2                    AS                    CORRESPONDENCE_STREET_ADDRESS_2 
		, CORRESPONDENCE_CITY_NAME                           AS                           CORRESPONDENCE_CITY_NAME 
		, CORRESPONDENCE_STATE_CODE                          AS                          CORRESPONDENCE_STATE_CODE 
		, CORRESPONDENCE_ZIP_CODE_NMBR                       AS                       CORRESPONDENCE_ZIP_CODE_NMBR 
		, CORRESPONDENCE_ZIP_CODE_PLS4_NMBR                  AS                  CORRESPONDENCE_ZIP_CODE_PLS4_NMBR 
		, CORRESPONDENCE_FULL_ZIP_CODE_NUMBER                AS                CORRESPONDENCE_FULL_ZIP_CODE_NUMBER 
		, CORRESPONDENCE_COUNTY_NAME                         AS                         CORRESPONDENCE_COUNTY_NAME 
		, NPI_NMBR                                           AS                                           NPI_NMBR 
		, NPI_CNFRM_IND                                      AS                                      NPI_CNFRM_IND 
		, NPI_EFCTV_DATE                                     AS                                     NPI_EFCTV_DATE 
		, MEDICARE_NUMBER                                    AS                                    MEDICARE_NUMBER 
		, MEDICARE_NUMBER_STATE_CODE                         AS                         MEDICARE_NUMBER_STATE_CODE 
		, MEDICARE_NUMBER_EFFECTIVE_DATE                     AS                     MEDICARE_NUMBER_EFFECTIVE_DATE 
		, MEDICARE_PROVIDER_TYPE_CODE                        AS                        MEDICARE_PROVIDER_TYPE_CODE 
		, MEDICARE_PROVIDER_TYPE_DESC                        AS                        MEDICARE_PROVIDER_TYPE_DESC 
		, CRITICAL_ACCESS_IND                                AS                                CRITICAL_ACCESS_IND 
		, OPPS_QUALITY_IND                                   AS                                   OPPS_QUALITY_IND 
		, PROVIDER_CBSA_CODE                                 AS                                 PROVIDER_CBSA_CODE 
		, CURRENT_ENROLLMENT_STATUS_DESC                     AS                     CURRENT_ENROLLMENT_STATUS_DESC 
		, CURRENT_CERTIFICATION_STATUS_DESC                  AS                  CURRENT_CERTIFICATION_STATUS_DESC 
		, NEW_PATIENT_CODE                                   AS                                   NEW_PATIENT_CODE 
		, NEW_PATIENT_DESC                                   AS                                   NEW_PATIENT_DESC 
		, PROVIDER_PROGRAM_CODE                              AS                              PROVIDER_PROGRAM_CODE 
		, PROVIDER_PROGRAM_DESC                              AS                              PROVIDER_PROGRAM_DESC 
		, PROVIDER_PROGRAM_FOCUS_TEXT                        AS                        PROVIDER_PROGRAM_FOCUS_TEXT 
		, PROGRAM_PARTICIPATION_PERIOD                       AS                       PROGRAM_PARTICIPATION_PERIOD 
		, DEP_SERVICE_CODE                                   AS                                   DEP_SERVICE_CODE 
		, DEP_SERVICE_DESC                                   AS                                   DEP_SERVICE_DESC 
		, DEP_STATUS_DESC                                    AS                                    DEP_STATUS_DESC 
		, DEP_ACTIVE_IND                                     AS                                     DEP_ACTIVE_IND 
		, DEP_90_DAY_EXAM_IND                                AS                                DEP_90_DAY_EXAM_IND 
		, DEP_ADR_IME_IND                                    AS                                    DEP_ADR_IME_IND 
		, DEP_ADR_FILE_REVIEW_IND                            AS                            DEP_ADR_FILE_REVIEW_IND 
		, DEP_B_READER_IND                                   AS                                   DEP_B_READER_IND 
		, DEP_C92_EXAM_IND                                   AS                                   DEP_C92_EXAM_IND 
		, DEP_C92A_FILE_REVIEW_IND                           AS                           DEP_C92A_FILE_REVIEW_IND 
		, DEP_DUR_REVIEW_IND                                 AS                                 DEP_DUR_REVIEW_IND 
		, DEP_IME_IND                                        AS                                        DEP_IME_IND 
		, DEP_MEDICAL_FILE_REVIEW_IND                        AS                        DEP_MEDICAL_FILE_REVIEW_IND 
		, DEP_PRIOR_AUTHORIZATION_REVIEW_IND                 AS                 DEP_PRIOR_AUTHORIZATION_REVIEW_IND 
		, DEP_DM_IME_IND                                     AS                                     DEP_DM_IME_IND 
		, DEP_ADDITIONAL_LANGUAGE_IND                        AS                        DEP_ADDITIONAL_LANGUAGE_IND 
		, DEP_ONLINE_FILE_REVIEW_IND                         AS                         DEP_ONLINE_FILE_REVIEW_IND 
		, DEP_PULMONARY_EXAM_IND                             AS                             DEP_PULMONARY_EXAM_IND 
		, DEP_PULMONARY_REVIEW_IND                           AS                           DEP_PULMONARY_REVIEW_IND 
		, DEP_ASBESTOSIS_EXAM_IND                            AS                            DEP_ASBESTOSIS_EXAM_IND 
		, MCO_MEDICAL_DIRECTOR_IND                           AS                           MCO_MEDICAL_DIRECTOR_IND 
		, DEP_ADMINISTRATIVE_AGENT_NAME                      AS                      DEP_ADMINISTRATIVE_AGENT_NAME 
		, PRDT_CRT_DTTM                                      AS                                      PRDT_CRT_DTTM 
		, PRDT_CRT_USER_CODE                                 AS                                 PRDT_CRT_USER_CODE 
		, PRDT_DCTVT_DTTM                                    AS                                    PRDT_DCTVT_DTTM 
		, PRDT_DCTVT_USER_CODE                               AS                               PRDT_DCTVT_USER_CODE 
		from SRC_PRVDR
            )

---- RENAME LAYER ----
,

RENAME_PRVDR as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PEACH_NUMBER                                       as                              PROVIDER_PEACH_NUMBER
		, PEACH_FORMATTED_NUMBER                             as                             PEACH_FORMATTED_NUMBER
		, PEACH_BASE_NUMBER                                  as                                  PEACH_BASE_NUMBER
		, PEACH_SUFFIX_NUMBER                                as                                PEACH_SUFFIX_NUMBER
		, PRVDR_LEGAL_NAME                                   as                                      PROVIDER_NAME
		, PRSN_IND                                           as                                         PERSON_IND
		, GNDR_CODE                                          as                                        GENDER_CODE
		, BIRTH_DATE                                         as                                         BIRTH_DATE
		, DEATH_DATE                                         as                                         DEATH_DATE
		, BSNS_TYPE_CODE                                     as                                 BUSINESS_TYPE_CODE
		, DBA_NAME                                           as                             PROVIDER_DBA_SORT_NAME
		, PRCT_TYPE_CODE                                     as                                 PRACTICE_TYPE_CODE
		, PRCT_TYPE_NAME                                     as                                 PRACTICE_TYPE_DESC
		, PRVDR_TYPE_CODE                                    as                                 PROVIDER_TYPE_CODE
		, PRVDR_TYPE_DESC                                    as                                 PROVIDER_TYPE_DESC
		, PROVIDER_SPECIALTY_LIST_TEXT                       as                       PROVIDER_SPECIALTY_LIST_TEXT
        , PROVIDER_PHONE_NUMBER                              as                              PROVIDER_PHONE_NUMBER
		, PROVIDER_FAX_NUMBER                                as                                PROVIDER_FAX_NUMBER
		, PROVIDER_EMAIL_ADDRESS                             as                             PROVIDER_EMAIL_ADDRESS
		, OUT_OF_STATE_STATUS_DESC                           as                           OUT_OF_STATE_STATUS_DESC
		, PRACTICE_STREET_ADDRESS_1                          as                          PRACTICE_STREET_ADDRESS_1
		, PRACTICE_STREET_ADDRESS_2                          as                          PRACTICE_STREET_ADDRESS_2
		, PRACTICE_CITY_NAME                                 as                              PRACTICE_ADDRESS_CITY
		, PRACTICE_STATE_CODE                                as                        PRACTICE_ADDRESS_STATE_CODE
		, PRACTICE_ZIP_CODE_NMBR                             as                           PRACTICE_ZIP_CODE_NUMBER
		, PRACTICE_ZIP_CODE_PLS4_NMBR                        as                              PRACTICE_ZIP_CODE_EXT
		, PRACTICE_FULL_ZIP_CODE_NUMBER                      as                             PRACTICE_FULL_ZIP_CODE
		, PRACTICE_COUNTY_NAME                               as                               PRACTICE_COUNTY_NAME
		, CORRESPONDENCE_STREET_ADDRESS_1                    as                    CORRESPONDENCE_STREET_ADDRESS_1
		, CORRESPONDENCE_STREET_ADDRESS_2                    as                    CORRESPONDENCE_STREET_ADDRESS_2
		, CORRESPONDENCE_CITY_NAME                           as                        CORRESPONDENCE_ADDRESS_CITY
		, CORRESPONDENCE_STATE_CODE                          as                  CORRESPONDENCE_ADDRESS_STATE_CODE
		, CORRESPONDENCE_ZIP_CODE_NMBR                       as                     CORRESPONDENCE_ZIP_CODE_NUMBER
		, CORRESPONDENCE_ZIP_CODE_PLS4_NMBR                  as                        CORRESPONDENCE_ZIP_CODE_EXT
		, CORRESPONDENCE_FULL_ZIP_CODE_NUMBER                as                       CORRESPONDENCE_FULL_ZIP_CODE
		, CORRESPONDENCE_COUNTY_NAME                         as                         CORRESPONDENCE_COUNTY_NAME
		, NPI_NMBR                                           as                                         NPI_NUMBER
		, NPI_CNFRM_IND                                      as                               NPI_CONFIRMATION_IND
		, NPI_EFCTV_DATE                                     as                                 NPI_EFFECTIVE_DATE
		, MEDICARE_NUMBER                                    as                                    MEDICARE_NUMBER
		, MEDICARE_NUMBER_STATE_CODE                         as                         MEDICARE_NUMBER_STATE_CODE
		, MEDICARE_NUMBER_EFFECTIVE_DATE                     as                     MEDICARE_NUMBER_EFFECTIVE_DATE
		, MEDICARE_PROVIDER_TYPE_CODE                        as                        MEDICARE_PROVIDER_TYPE_CODE
		, MEDICARE_PROVIDER_TYPE_DESC                        as                        MEDICARE_PROVIDER_TYPE_DESC
		, CRITICAL_ACCESS_IND                                as                                CRITICAL_ACCESS_IND
		, OPPS_QUALITY_IND                                   as                                   OPPS_QUALITY_IND
		, PROVIDER_CBSA_CODE                                 as                                 PROVIDER_CBSA_CODE
		, CURRENT_ENROLLMENT_STATUS_DESC                     as                          CURRENT_ENROLLMENT_STATUS
		, CURRENT_CERTIFICATION_STATUS_DESC                  as                       CURRENT_CERTIFICATION_STATUS
		, NEW_PATIENT_CODE                                   as                                   NEW_PATIENT_CODE
		, NEW_PATIENT_DESC                                   as                                   NEW_PATIENT_DESC
		, PROVIDER_PROGRAM_CODE                              as                              PROVIDER_PROGRAM_CODE
		, PROVIDER_PROGRAM_DESC                              as                              PROVIDER_PROGRAM_DESC
		, PROVIDER_PROGRAM_FOCUS_TEXT                        as                        PROVIDER_PROGRAM_FOCUS_TEXT
		, PROGRAM_PARTICIPATION_PERIOD                       as                       PROGRAM_PARTICIPATION_PERIOD
		, DEP_SERVICE_CODE                                   as                                   DEP_SERVICE_CODE
		, DEP_SERVICE_DESC                                   as                                   DEP_SERVICE_DESC
		, DEP_STATUS_DESC                                    as                                    DEP_STATUS_DESC
		, DEP_ACTIVE_IND                                     as                                     DEP_ACTIVE_IND
		, DEP_90_DAY_EXAM_IND                                as                                DEP_90_DAY_EXAM_IND
		, DEP_ADR_IME_IND                                    as                                    DEP_ADR_IME_IND
		, DEP_ADR_FILE_REVIEW_IND                            as                            DEP_ADR_FILE_REVIEW_IND
		, DEP_B_READER_IND                                   as                                   DEP_B_READER_IND
		, DEP_C92_EXAM_IND                                   as                                   DEP_C92_EXAM_IND
		, DEP_C92A_FILE_REVIEW_IND                           as                           DEP_C92A_FILE_REVIEW_IND
		, DEP_DUR_REVIEW_IND                                 as                                 DEP_DUR_REVIEW_IND
		, DEP_IME_IND                                        as                                        DEP_IME_IND
		, DEP_MEDICAL_FILE_REVIEW_IND                        as                        DEP_MEDICAL_FILE_REVIEW_IND
		, DEP_PRIOR_AUTHORIZATION_REVIEW_IND                 as                 DEP_PRIOR_AUTHORIZATION_REVIEW_IND
		, DEP_DM_IME_IND                                     as                                     DEP_DM_IME_IND
		, DEP_ADDITIONAL_LANGUAGE_IND                        as                        DEP_ADDITIONAL_LANGUAGE_IND
		, DEP_ONLINE_FILE_REVIEW_IND                         as                         DEP_ONLINE_FILE_REVIEW_IND
		, DEP_PULMONARY_EXAM_IND                             as                             DEP_PULMONARY_EXAM_IND
		, DEP_PULMONARY_REVIEW_IND                           as                           DEP_PULMONARY_REVIEW_IND
		, DEP_ASBESTOSIS_EXAM_IND                            as                            DEP_ASBESTOSIS_EXAM_IND
		, MCO_MEDICAL_DIRECTOR_IND                           as                           MCO_MEDICAL_DIRECTOR_IND
		, DEP_ADMINISTRATIVE_AGENT_NAME                      as                      DEP_ADMINISTRATIVE_AGENT_NAME
		, PRDT_CRT_DTTM                                      as                       PROVIDER_DETAILS_CREATE_DTTM
		, PRDT_CRT_USER_CODE                                 as                  PROVIDER_DETAILS_CREATE_USER_CODE
		, PRDT_DCTVT_DTTM                                    as                   PROVIDER_DETAILS_DEACTIVATE_DTTM
		, PRDT_DCTVT_USER_CODE                               as              PROVIDER_DETAILS_DEACTIVATE_USER_CODE 
				FROM     LOGIC_PRVDR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PRVDR                          as ( SELECT * from    RENAME_PRVDR   ),

---- JOIN LAYER ----

 JOIN_PRVDR  as  ( SELECT * 
				FROM  FILTER_PRVDR )
 SELECT * FROM  JOIN_PRVDR