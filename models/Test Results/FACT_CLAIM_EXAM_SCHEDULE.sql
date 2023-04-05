{{ config( 
tags = [ "fact" ] 
,	 post_hook = ("
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  PRIMARY KEY (CASE_NUMBER,SCHEDULE_NUMBER);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_NUMBER SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY SCHEDULE_NUMBER SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_STATUS_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_OWNER_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_INITIATION_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_EFFECTIVE_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_DUE_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_TYPE_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_REQUEST_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY ASSIGNED_TEAM_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY REFERRAL_CREATE_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_REFERRAL_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY IW_EXAM_SCHEDULE_GUIDE_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY IW_EXAM_AVAILABILITY_NOTE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_SCHEDULE_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_SCHEDULE_TIME_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY SCHEDULED_EXAM_PHYSICIAN_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_LOCATION_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_SCHEDULER_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EXAM_REPORT_RECEIVED_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CASE_COMPLETE_DATE_KEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY EMPLOYER_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY POLICY_STANDING_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY INJURED_WORKER_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CLAIM_TYPE_STATUS_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CLAIM_DETAIL_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY CLAIM_ACCIDENT_DESC_HKEY SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY LOAD_DATETIME SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  MODIFY PRIMARY_SOURCE_SYSTEM SET NOT NULL ;
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_SCHEDULE_fk FOREIGN KEY (IW_EXAM_SCHEDULE_GUIDE_HKEY) REFERENCES DIMENSIONS.DIM_IW_EXAM_SCHEDULE_GUIDE (IW_EXAM_SCHEDULE_GUIDE_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_CASE_DETAIL_fk FOREIGN KEY (EXAM_REQUEST_HKEY) REFERENCES DIMENSIONS.DIM_EXAM_REQUEST (EXAM_REQUEST_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CASE_TYPE_fk FOREIGN KEY (CASE_TYPE_HKEY) REFERENCES DIMENSIONS.DIM_CASE_TYPE (CASE_TYPE_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CASE_EFFECTIVE_DATE_fk FOREIGN KEY (CASE_EFFECTIVE_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CASE_DUE_DATE_fk FOREIGN KEY (CASE_DUE_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CASE_COMPLETE_DATE_fk FOREIGN KEY (CASE_COMPLETE_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_DATE_DATE_fk FOREIGN KEY (EXAM_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_REPORT_RECEIVED_fk FOREIGN KEY (EXAM_REPORT_RECEIVED_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_POLICY_STANDING_fk FOREIGN KEY (POLICY_STANDING_HKEY) REFERENCES DIMENSIONS.DIM_POLICY_STANDING (POLICY_STANDING_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CLAIM_ACCIDENT_DESC_fk FOREIGN KEY (CLAIM_ACCIDENT_DESC_HKEY) REFERENCES DIMENSIONS.DIM_CLAIM_ACCIDENT_DESCRIPTION (CLAIM_ACCIDENT_DESC_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_INJURED_WORKER_fk FOREIGN KEY (INJURED_WORKER_HKEY) REFERENCES DIMENSIONS.DIM_INJURED_WORKER (INJURED_WORKER_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EMPLOYER_fk FOREIGN KEY (EMPLOYER_HKEY) REFERENCES DIMENSIONS.DIM_EMPLOYER (EMPLOYER_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CLAIM_TYPE_STATUS_fk FOREIGN KEY (CLAIM_TYPE_STATUS_HKEY) REFERENCES DIMENSIONS.DIM_CLAIM_TYPE_STATUS (CLAIM_TYPE_STATUS_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CLAIM_DETAIL_fk FOREIGN KEY (CLAIM_DETAIL_HKEY) REFERENCES DIMENSIONS.DIM_CLAIM_DETAIL (CLAIM_DETAIL_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_REFERRAL_DATE_fk FOREIGN KEY (EXAM_REFERRAL_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CASE_OWNER_fk FOREIGN KEY (CASE_OWNER_HKEY) REFERENCES DIMENSIONS.DIM_USER (USER_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_CASE_INITIATION_DATE_fk FOREIGN KEY (CASE_INITIATION_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_ASSIGNED_TEAM_fk FOREIGN KEY (ASSIGNED_TEAM_HKEY) REFERENCES DIMENSIONS.DIM_ORGANIZATIONAL_UNIT (ORGANIZATION_UNIT_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_SCHEDULE_TIME_fk FOREIGN KEY (EXAM_SCHEDULE_TIME_KEY) REFERENCES DIMENSIONS.DIM_TIME (TIME_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_PHYSICIAN_fk FOREIGN KEY (SCHEDULED_EXAM_PHYSICIAN_HKEY) REFERENCES DIMENSIONS.DIM_PROVIDER (PROVIDER_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_LOCATION_fk FOREIGN KEY (EXAM_LOCATION_HKEY) REFERENCES DIMENSIONS.DIM_LOCATION (LOCATION_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_SCHEDULER_fk FOREIGN KEY (EXAM_SCHEDULER_HKEY) REFERENCES DIMENSIONS.DIM_USER (USER_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_SCHEDULE_DATE_fk FOREIGN KEY (EXAM_SCHEDULE_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_IW_AVAILABLITY_SPECIAL_REQUIREMENT_NOTE_fk FOREIGN KEY (IW_EXAM_AVAILABILITY_NOTE_KEY) REFERENCES DIMENSIONS.DIM_IW_AVAILABILITY_SPECIAL_REQUIREMENT (AVAILABILITY_SPECIAL_REQUIREMENT_HKEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_REFERRAL_CREATE_DATE_fk FOREIGN KEY (REFERRAL_CREATE_DATE_KEY) REFERENCES DIMENSIONS.DIM_DATE (DATE_KEY);
ALTER TABLE EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE ADD  CONSTRAINT CLAIM_EXAM_SCHEDULE_EXAM_CASE_STATUS_fk FOREIGN KEY (CASE_STATUS_HKEY) REFERENCES DIMENSIONS.DIM_CASE_STATUS (CASE_STATUS_HKEY);

") 
) }}

---- SRC LAYER ----
WITH

SRC_CES            as ( SELECT *     FROM     {{ ref( 'DSV_CLAIM_EXAM_SCHEDULE' ) }} ),
SRC_PRVDR          as ( SELECT *     FROM     {{ ref( 'DIM_PROVIDER' ) }} ),
SRC_CLM            as ( SELECT *     FROM     {{ ref( 'DSV_CLAIM' ) }} ),
SRC_EMP            as ( SELECT *     FROM     {{ ref( 'DIM_EMPLOYER' ) }} ),
SRC_IW             as ( SELECT *     FROM     {{ ref( 'DIM_INJURED_WORKER' ) }} ),

/*
SRC_CES            as ( SELECT *     FROM     STAGING.DSV_CLAIM_EXAM_SCHEDULE ),
SRC_PRVDR          as ( SELECT *     FROM     EDW.DIM_PROVIDER ),
SRC_CLM            as ( SELECT *     FROM     STAGING.DSV_CLAIM ),
SRC_EMP            as ( SELECT *     FROM     EDW.DIM_EMPLOYER ),
SRC_IW             as ( SELECT *     FROM     EDW.DIM_INJURED_WORKER ),

*/

---- LOGIC LAYER ----


, LOGIC_CES as ( 
	SELECT 	  
		 CASE_NUMBER                                        as                                        CASE_NUMBER	, 
		 SCHEDULE_NUMBER                                    as                                    SCHEDULE_NUMBER	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( CASE_STATE_CODE, CASE_STATUS_CODE, CASE_STATE_STATUS_REASON_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CASE_STATE_CODE','CASE_STATUS_CODE','CASE_STATE_STATUS_REASON_CODE' ] ) }} 
				END                                         
                                                             as                                   CASE_STATUS_HKEY	, 
		 CASE WHEN CLM_ENTRY_USER_LGN_NM is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CLM_ENTRY_USER_LGN_NM' ] ) }} 
				END                                         
                                                             as                                    CASE_OWNER_HKEY	, 
		 CASE WHEN CASE_INITIATION_DATE is null then '-1' 
			WHEN CASE_INITIATION_DATE < '1901-01-01' then '-2' 
			WHEN CASE_INITIATION_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_INITIATION_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                               CASE_INITIATION_DATE	, 
		 CASE WHEN CASE_EFFECTIVE_DATE is null then '-1' 
			WHEN CASE_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN CASE_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                CASE_EFFECTIVE_DATE	, 
		 CASE WHEN CASE_DUE_DATE is null then '-1' 
			WHEN CASE_DUE_DATE < '1901-01-01' then '-2' 
			WHEN CASE_DUE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_DUE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                      CASE_DUE_DATE	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( CASE_TYPE_CODE, CASE_CATEGORY_CODE, CONTEXT_TYPE_CODE, CASE_PRIORITY_CODE, CASE_RESOLVE_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CASE_TYPE_CODE','CASE_CATEGORY_CODE','CONTEXT_TYPE_CODE','CASE_PRIORITY_CODE','CASE_RESOLVE_CODE' ] ) }} 
				END                                         
                                                             as                                     CASE_TYPE_HKEY	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( EXAM_TYPE_CODE, CASE_PROFILE_CATEGORY_CODE, EXAM_REQUESTOR_TYPE_CODE, PHYSICIAN_SPECIALTY_NEEDED_CODE, SECOND_CHOICE_PHYSICIAN_SPECIALTY_CODE, ADDENDUM_REQUEST_TYPE_CODE, LANGUAGE_TYPE_CODE, EXAM_RESCHEDULE_STATUS_REASON_CODE, REFERRED_TO ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'EXAM_TYPE_CODE','CASE_PROFILE_CATEGORY_CODE','EXAM_REQUESTOR_TYPE_CODE','PHYSICIAN_SPECIALTY_NEEDED_CODE','SECOND_CHOICE_PHYSICIAN_SPECIALTY_CODE','ADDENDUM_REQUEST_TYPE_CODE','LANGUAGE_TYPE_CODE','EXAM_RESCHEDULE_STATUS_REASON_CODE','REFERRED_TO' ] ) }} 
				END                                         
                                                             as                                  EXAM_REQUEST_HKEY	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( ORGANIZATIONAL_UNIT_NAME, USER_FUNCTIONAL_ROLE_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ORGANIZATIONAL_UNIT_NAME','USER_FUNCTIONAL_ROLE_CODE' ] ) }} 
				END                                         
                                                             as                                 ASSIGNED_TEAM_HKEY	, 
		 CASE WHEN REFERRAL_CREATE_DATE is null then '-1' 
			WHEN REFERRAL_CREATE_DATE < '1901-01-01' then '-2' 
			WHEN REFERRAL_CREATE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( REFERRAL_CREATE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                               REFERRAL_CREATE_DATE	, 
		 CASE WHEN EXAM_REFERRAL_DATE is null then '-1' 
			WHEN EXAM_REFERRAL_DATE < '1901-01-01' then '-2' 
			WHEN EXAM_REFERRAL_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EXAM_REFERRAL_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                 EXAM_REFERRAL_DATE	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( MONDAY_AVAILABILITY_IND, TUESDAY_AVAILABILITY_IND, WEDNESDAY_AVAILABILITY_IND, THURSDAY_AVAILABILITY_IND, FRIDAY_AVAILABILITY_IND, SATURDAY_AVAILABILITY_IND, SUNDAY_AVAILABILITY_IND, INTERPRETER_NEEDED_IND, GREATER_THAN_45_MILES_IND, TRAVEL_REIMBURSEMENT_IND, ADDITIONAL_TESTING_IND, ADDENDUM_REQUESTED_IND, RESULT_SUSPENDED_IND, NO_SHOW_IND, RESCHEDULE_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'MONDAY_AVAILABILITY_IND','TUESDAY_AVAILABILITY_IND','WEDNESDAY_AVAILABILITY_IND','THURSDAY_AVAILABILITY_IND','FRIDAY_AVAILABILITY_IND','SATURDAY_AVAILABILITY_IND','SUNDAY_AVAILABILITY_IND','INTERPRETER_NEEDED_IND','GREATER_THAN_45_MILES_IND','TRAVEL_REIMBURSEMENT_IND','ADDITIONAL_TESTING_IND','ADDENDUM_REQUESTED_IND','RESULT_SUSPENDED_IND','NO_SHOW_IND','RESCHEDULE_IND' ] ) }} 
				END                                         
                                                             as                        IW_EXAM_SCHEDULE_GUIDE_HKEY	, 
		 CASE WHEN AVAILABILITY_SPECIAL_REQUIREMENT is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'AVAILABILITY_SPECIAL_REQUIREMENT' ] ) }} 
				END                                         
                                                             as                      IW_EXAM_AVAILABILITY_NOTE_KEY	, 
		 CASE WHEN EXAM_SCHEDULE_DATE is null then '-1' 
			WHEN EXAM_SCHEDULE_DATE < '1901-01-01' then '-2' 
			WHEN EXAM_SCHEDULE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EXAM_SCHEDULE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                 EXAM_SCHEDULE_DATE	, 
		 EXAM_SCHEDULE_DATETIME                             as                             EXAM_SCHEDULE_DATETIME	, 
		 CASE WHEN EXAM_DATE is null then '-1' 
			WHEN EXAM_DATE < '1901-01-01' then '-2' 
			WHEN EXAM_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EXAM_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                          EXAM_DATE	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( LOCATION_STRING_1, LOCATION_STRING_2, LOCATION_CITY, LOCATION_STATE, LOCATION_ZIP_CODE, LOCATION_COUNTY, LOCATION_COUNTRY, OUT_OF_STATE_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'LOCATION_STRING_1','LOCATION_STRING_2','LOCATION_CITY','LOCATION_STATE','LOCATION_ZIP_CODE','LOCATION_COUNTY','LOCATION_COUNTRY','OUT_OF_STATE_IND' ] ) }} 
				END                                         
                                                             as                                 EXAM_LOCATION_HKEY	, 
		 CASE WHEN EXAM_SCHDL_USER_LGN_NM is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'EXAM_SCHDL_USER_LGN_NM' ] ) }} 
				END                                         
                                                             as                                EXAM_SCHEDULER_HKEY	, 
		 CASE WHEN EXAM_REPORT_RECEIVED_DATE is null then '-1' 
			WHEN EXAM_REPORT_RECEIVED_DATE < '1901-01-01' then '-2' 
			WHEN EXAM_REPORT_RECEIVED_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EXAM_REPORT_RECEIVED_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                          EXAM_REPORT_RECEIVED_DATE	, 
		 CASE WHEN CASE_COMPLETE_DATE is null then '-1' 
			WHEN CASE_COMPLETE_DATE < '1901-01-01' then '-2' 
			WHEN CASE_COMPLETE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_COMPLETE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                 CASE_COMPLETE_DATE	, 
		 RELATED_CASE_NUMBER                                as                                RELATED_CASE_NUMBER	, 
		 CASE_EXTERNAL_NUMBER                               as                               CASE_EXTERNAL_NUMBER	, 
		 CLAIM_NUMBER                                       as                                       CLAIM_NUMBER	, 
		 CASE_ID                                            as                                            CASE_ID	, 
		 CALCULATE NO OF DAYS BETWEEN NVL(EXAM_REFERRAL_DATE , CASE_EFFECTIVE_DATE) AND EXAM_SCHEDULE_DATE.

NOTE: CALCULATE ONLY THE WORKING DAYS(I.E. EXCLUDE WEEKENDS AND HOLIDAYS). USE A DSV_DATE TO FIND THE WORKING DAYS. EXCLUDE THE DAYS WHERE HOLIDAY_IND ='Y' & WEEKEND_IND='Y'
                                                             as           REFERRAL_TO_EXAM_SCHEDULED_LAG_DAY_COUNT	, 
		 CALCULATE NO OF DAYS BETWEEN EXAM_SCHEDULE_DATE AND EXAM_DATE.

NOTE: CALCULATE ONLY THE WORKING DAYS(I.E. EXCLUDE WEEKENDS AND HOLIDAYS). USE A DSV_DATE TO FIND THE WORKING DAYS. EXCLUDE THE DAYS WHERE HOLIDAY_IND ='Y' & WEEKEND_IND='Y'
                                                             as               EXAM_SCHEDULED_TO_EXAM_LAG_DAY_COUNT	, 
		 CALCULATE NO OF DAYS BETWEEN EXAM_DATE AND EXAM_REPORT_RECEIVED_DATE.

NOTE: CALCULATE ONLY THE WORKING DAYS(I.E. EXCLUDE WEEKENDS AND HOLIDAYS). USE A DSV_DATE TO FIND THE WORKING DAYS. EXCLUDE THE DAYS WHERE HOLIDAY_IND ='Y' & WEEKEND_IND='Y'
                                                             as              EXAM_TO_REPORT_RECEIVED_LAG_DAY_COUNT	, 
		 NO_SHOW_COUNT                                      as                                      NO_SHOW_COUNT	, 
		 EXAM_RESCHDL_COUNT                                 as                                 EXAM_RESCHDL_COUNT	, 
		 CURRENT_EXAM_SCHEDULE_IND                          as                          CURRENT_EXAM_SCHEDULE_IND	, 
		 PRVDR_PEACH_NUMBER                                 as                                 PRVDR_PEACH_NUMBER	, 
		 CLM_AGRE_ID                                        as                                        CLM_AGRE_ID	, 
		 ZIP_CODE_LONGITUDE                                 as                                 ZIP_CODE_LONGITUDE	, 
		 ZIP_CODE_LATITUDE                                  as                                  ZIP_CODE_LATITUDE
	 from SRC_CES )

, LOGIC_PRVDR as ( 
	SELECT 	  
		 PROVIDER_HKEY                                      as                                      PROVIDER_HKEY	, 
		 PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER	, 
		 RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE	, 
		 RECORD_END_DATE                                    as                                    RECORD_END_DATE	, 
		 PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER	, 
		 RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE	, 
		 RECORD_END_DATE                                    as                                    RECORD_END_DATE
	 from SRC_PRVDR )

, LOGIC_CLM as ( 
	SELECT 	  
		 CASE WHEN  nullif(array_to_string(array_construct_compact( POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'POLICY_TYPE_CODE','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','POLICY_ACTIVE_IND' ] ) }} 
				END                                         
                                                             as                               POLICY_STANDING_HKEY	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( CURRENT_CORESUITE_CLAIM_TYPE_CODE, CLAIM_TYPE_CHNG_OVR_IND, CLAIM_STATE_CODE, CLAIM_STATUS_CODE, CLAIM_STATUS_REASON_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'CURRENT_CORESUITE_CLAIM_TYPE_CODE','CLAIM_TYPE_CHNG_OVR_IND','CLAIM_STATE_CODE','CLAIM_STATUS_CODE','CLAIM_STATUS_REASON_CODE' ] ) }} 
				END                                         
                                                             as                             CLAIM_TYPE_STATUS_HKEY	, 
		  {{ dbt_utils.generate_surrogate_key ( [ 'FILING_SOURCE_DESC','FILING_MEDIA_DESC','NATURE_OF_INJURY_CATEGORY','NATURE_OF_INJURY_TYPE','FIREFIGHTER_CANCER_IND','COVID_EXPOSURE_IND','COVID_EMERGENCY_WORKER_IND','COVID_HEALTH_CARE_WORKER_IND','COMBINED_IND','SB223_IND','EMPLOYER_PREMISES_IND','CATASTROPHIC_IND','K_PROGRAM_ENROLLMENT_DESC','K_PROGRAM_TYPE_DESC','K_PROGRAM_REASON_DESC' ] ) }} 
                                                             as                                  CLAIM_DETAIL_HKEY	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( ACCIDENT_DESCRIPTION_TEXT, IW_JOB_TITLE ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ACCIDENT_DESCRIPTION_TEXT','IW_JOB_TITLE' ] ) }} 
				END                                         
                                                             as                           CLAIM_ACCIDENT_DESC_HKEY	, 
		 POLICY_NUMBER                                      as                                      POLICY_NUMBER	, 
		 AGRE_ID                                            as                                            AGRE_ID
	 from SRC_CLM )

, LOGIC_EMP as ( 
	SELECT 	  
		 EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY	, 
		 CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER	, 
		 CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
	 from SRC_EMP )

, LOGIC_IW as ( 
	SELECT 	  
		 INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY	, 
		 CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER	, 
		 RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE	, 
		 RECORD_END_DATE                                    as                                    RECORD_END_DATE	, 
		 MAILING_ZIP_CODE_LONGITUDE                         as                         MAILING_ZIP_CODE_LONGITUDE	, 
		 MAILING_ZIP_CODE_LATITUDE                          as                          MAILING_ZIP_CODE_LATITUDE
	 from SRC_IW )


---- RENAME LAYER ----


, RENAME_CES as ( SELECT  
		 CASE_NUMBER                                        as                                        CASE_NUMBER , 
		 SCHEDULE_NUMBER                                    as                                    SCHEDULE_NUMBER , 
		 CASE_STATUS_HKEY                                   as                                   CASE_STATUS_HKEY , 
		 CASE_OWNER_HKEY                                    as                                    CASE_OWNER_HKEY , 
		 CASE_INITIATION_DATE                               as                           CASE_INITIATION_DATE_KEY , 
		 CASE_EFFECTIVE_DATE                                as                            CASE_EFFECTIVE_DATE_KEY , 
		 CASE_DUE_DATE                                      as                                  CASE_DUE_DATE_KEY , 
		 CASE_TYPE_HKEY                                     as                                     CASE_TYPE_HKEY , 
		 EXAM_REQUEST_HKEY                                  as                                  EXAM_REQUEST_HKEY , 
		 ASSIGNED_TEAM_HKEY                                 as                                 ASSIGNED_TEAM_HKEY , 
		 REFERRAL_CREATE_DATE                               as                           REFERRAL_CREATE_DATE_KEY , 
		 EXAM_REFERRAL_DATE                                 as                             EXAM_REFERRAL_DATE_KEY , 
		 IW_EXAM_SCHEDULE_GUIDE_HKEY                        as                        IW_EXAM_SCHEDULE_GUIDE_HKEY , 
		 IW_EXAM_AVAILABILITY_NOTE_KEY                      as                      IW_EXAM_AVAILABILITY_NOTE_KEY , 
		 EXAM_SCHEDULE_DATE                                 as                             EXAM_SCHEDULE_DATE_KEY , 
		 EXAM_SCHEDULE_DATETIME                             as                             EXAM_SCHEDULE_TIME_KEY , 
		 EXAM_DATE                                          as                                      EXAM_DATE_KEY , 
		 EXAM_LOCATION_HKEY                                 as                                 EXAM_LOCATION_HKEY , 
		 EXAM_SCHEDULER_HKEY                                as                                EXAM_SCHEDULER_HKEY , 
		 EXAM_REPORT_RECEIVED_DATE                          as                      EXAM_REPORT_RECEIVED_DATE_KEY , 
		 CASE_COMPLETE_DATE                                 as                             CASE_COMPLETE_DATE_KEY , 
		 RELATED_CASE_NUMBER                                as                                RELATED_CASE_NUMBER , 
		 CASE_EXTERNAL_NUMBER                               as                               CASE_EXTERNAL_NUMBER , 
		 CLAIM_NUMBER                                       as                                       CLAIM_NUMBER , 
		 CASE_ID                                            as                                            CASE_ID , 
		 REFERRAL_TO_EXAM_SCHEDULED_LAG_DAY_COUNT           as           REFERRAL_TO_EXAM_SCHEDULED_LAG_DAY_COUNT , 
		 EXAM_SCHEDULED_TO_EXAM_LAG_DAY_COUNT               as               EXAM_SCHEDULED_TO_EXAM_LAG_DAY_COUNT , 
		 EXAM_TO_REPORT_RECEIVED_LAG_DAY_COUNT              as              EXAM_TO_REPORT_RECEIVED_LAG_DAY_COUNT , 
		 NO_SHOW_COUNT                                      as                                      NO_SHOW_COUNT , 
		 EXAM_RESCHDL_COUNT                                 as                                   RESCHEDULE_COUNT , 
		 CURRENT_EXAM_SCHEDULE_IND                          as                          CURRENT_EXAM_SCHEDULE_IND , 
		 PRVDR_PEACH_NUMBER                                 as                                 PRVDR_PEACH_NUMBER , 
		 CLM_AGRE_ID                                        as                                        CLM_AGRE_ID , 
		 ZIP_CODE_LONGITUDE                                 as                                 ZIP_CODE_LONGITUDE , 
		 ZIP_CODE_LATITUDE                                  as                                  ZIP_CODE_LATITUDE 
		FROM LOGIC_CES
            )

, RENAME_PRVDR as ( SELECT  
		 PROVIDER_HKEY                                      as                      SCHEDULED_EXAM_PHYSICIAN_HKEY , 
		 PROVIDER_PEACH_NUMBER                              as                        PRVDR_PROVIDER_PEACH_NUMBER , 
		 RECORD_EFFECTIVE_DATE                              as                        PRVDR_RECORD_EFFECTIVE_DATE , 
		 RECORD_END_DATE                                    as                              PRVDR_RECORD_END_DATE , 
		 PROVIDER_PEACH_NUMBER                              as                        PRVDR_PROVIDER_PEACH_NUMBER , 
		 RECORD_EFFECTIVE_DATE                              as                        PRVDR_RECORD_EFFECTIVE_DATE , 
		 RECORD_END_DATE                                    as                              PRVDR_RECORD_END_DATE 
		FROM LOGIC_PRVDR
            )

, RENAME_EMP as ( SELECT  
		 EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY , 
		 CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER , 
		 CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		FROM LOGIC_EMP
            )

, RENAME_CLM as ( SELECT  
		 POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY , 
		 CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY , 
		 CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY , 
		 CLAIM_ACCIDENT_DESC_HKEY                           as                           CLAIM_ACCIDENT_DESC_HKEY , 
		 POLICY_NUMBER                                      as                                      POLICY_NUMBER , 
		 AGRE_ID                                            as                                            AGRE_ID 
		FROM LOGIC_CLM
            )

, RENAME_IW as ( SELECT  
		 INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY , 
		 CUSTOMER_NUMBER                                    as                       IW_CORESUITE_CUSTOMER_NUMBER , 
		 RECORD_EFFECTIVE_DATE                              as                           IW_RECORD_EFFECTIVE_DATE , 
		 RECORD_END_DATE                                    as                                 IW_RECORD_END_DATE , 
		 MAILING_ZIP_CODE_LONGITUDE                         as                         MAILING_ZIP_CODE_LONGITUDE , 
		 MAILING_ZIP_CODE_LATITUDE                          as                          MAILING_ZIP_CODE_LATITUDE 
		FROM LOGIC_IW
            )

---- FILTER LAYER ----

FILTER_CES                            as ( SELECT * FROM    RENAME_CES    ),
FILTER_PRVDR                          as ( SELECT * FROM    RENAME_PRVDR    ),
FILTER_CLM                            as ( SELECT * FROM    RENAME_CLM    ),
FILTER_EMP                            as ( SELECT * FROM    RENAME_EMP    ),
FILTER_IW                             as ( SELECT * FROM    RENAME_IW    )

---- JOIN LAYER ----

CLM as ( SELECT * 
				FROM  FILTER_CLM
				LEFT JOIN FILTER_EMP ON  coalesce( FILTER_CLM.EMP_CUSTOMER_NUMBER, '99999') =  FILTER_EMP.CUSTOMER_NUMBER AND  CURRENT_RECORD_IND = 'Y' 
								LEFT JOIN FILTER_IW ON  coalesce( FILTER_CLM.IW_CUSTOMER_NUMBER, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND COALESCE(EXAM_SCHEDULE_DATE, CURRENT_DATE) BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31')  ),
CES as ( SELECT * 
				FROM  FILTER_CES
				LEFT JOIN FILTER_CLM ON  FILTER_CES.CLM_AGRE_ID =  FILTER_AGRE_ID 
						LEFT JOIN FILTER_PRVDR ON  coalesce( FILTER_CES.PRVDR_PEACH_NUMBER '99999999999') =  FILTER_PRVDR.PRVDR_PROVIDER_PEACH_NUMBER AND COALESCE(EXAM_SCHEDULE_DATE, CURRENT_DATE) BETWEEN PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( PRVDR_RECORD_END_DATE, '2099-12-31')  )
SELECT 
		  CASE_NUMBER
		, SCHEDULE_NUMBER
		, CASE_STATUS_HKEY
		, CASE_OWNER_HKEY
		, CASE_INITIATION_DATE_KEY
		, CASE_EFFECTIVE_DATE_KEY
		, CASE_DUE_DATE_KEY
		, CASE_TYPE_HKEY
		, EXAM_REQUEST_HKEY
		, ASSIGNED_TEAM_HKEY
		, REFERRAL_CREATE_DATE_KEY
		, EXAM_REFERRAL_DATE_KEY
		, IW_EXAM_SCHEDULE_GUIDE_HKEY
		, IW_EXAM_AVAILABILITY_NOTE_KEY
		, EXAM_SCHEDULE_DATE_KEY
		, EXAM_SCHEDULE_TIME_KEY
		, EXAM_DATE_KEY
		, SCHEDULED_EXAM_PHYSICIAN_HKEY
		, EXAM_LOCATION_HKEY
		, EXAM_SCHEDULER_HKEY
		, EXAM_REPORT_RECEIVED_DATE_KEY
		, CASE_COMPLETE_DATE_KEY
		, EMPLOYER_HKEY
		, POLICY_STANDING_HKEY
		, INJURED_WORKER_HKEY
		, CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY
		, CLAIM_ACCIDENT_DESC_HKEY
		, RELATED_CASE_NUMBER
		, CASE_EXTERNAL_NUMBER
		, CLAIM_NUMBER
		, POLICY_NUMBER
		, CASE_ID
		, REFERRAL_TO_EXAM_SCHEDULED_LAG_DAY_COUNT
		, EXAM_SCHEDULED_TO_EXAM_LAG_DAY_COUNT
		, EXAM_TO_REPORT_RECEIVED_LAG_DAY_COUNT
		, NO_SHOW_COUNT
		, RESCHEDULE_COUNT
		, 
		 IF CSE.LOCATION_STRING_1 IS NOT NULL OR LOCATION_CITY IS NOT NULL THEN USE THE BELOW LOGIC

ROUND(ST_DISTANCE(ST_MAKEPOINT(IW.MAILING_ZIP_CODE_LONGITUDE, IW.MAILING_ZIP_CODE_LATITUDE), ST_MAKEPOINT(CES.ZIP_CODE_LONGITUDE, ZIP_CODE_LATITUDE)) /1609 , 2)
                                                             as           SCHEDULED_EXAM_ESTIMATED_TRAVEL_DISTANCE
		, CURRENT_EXAM_SCHEDULE_IND
		, 
		                                                    as                                                   
		, 
		                                                    as                                                   
		, 
		                                                    as                                                    
FROM CES