

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_PROVIDER  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(PROVIDER_PEACH_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROVIDER_PEACH_NUMBER, 
     last_value(PEACH_FORMATTED_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PEACH_FORMATTED_NUMBER, 
     last_value(PEACH_BASE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PEACH_BASE_NUMBER, 
     last_value(PEACH_SUFFIX_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PEACH_SUFFIX_NUMBER, 
     last_value(PRACTICE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_TYPE_DESC, 
     last_value(PROVIDER_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROVIDER_TYPE_CODE, 
     last_value(PROVIDER_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROVIDER_TYPE_DESC, 
     last_value(PROVIDER_SPECIALTY_LIST_TEXT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROVIDER_SPECIALTY_LIST, 
          last_value(PROVIDER_PHONE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROVIDER_PHONE_NUMBER, 
     last_value(OUT_OF_STATE_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as OUT_OF_STATE_STATUS_DESC, 
     last_value(PRACTICE_STREET_ADDRESS_1) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_STREET_ADDRESS_1, 
     last_value(PRACTICE_STREET_ADDRESS_2) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_STREET_ADDRESS_2, 
     last_value(PRACTICE_ADDRESS_CITY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_ADDRESS_CITY, 
     last_value(PRACTICE_ADDRESS_STATE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_ADDRESS_STATE_CODE, 
     last_value(PRACTICE_ZIP_CODE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_ZIP_CODE_NUMBER, 
     last_value(PRACTICE_ZIP_CODE_EXT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_ZIP_CODE_EXT, 
     last_value(PRACTICE_FULL_ZIP_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_FULL_ZIP_CODE, 
     last_value(PRACTICE_COUNTY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRACTICE_COUNTY_NAME, 
     last_value(CORRESPONDENCE_STREET_ADDRESS_1) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_STREET_ADDRESS_1, 
     last_value(CORRESPONDENCE_STREET_ADDRESS_2) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_STREET_ADDRESS_2, 
     last_value(CORRESPONDENCE_ADDRESS_CITY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_ADDRESS_CITY, 
     last_value(CORRESPONDENCE_ADDRESS_STATE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_ADDRESS_STATE_CODE, 
     last_value(CORRESPONDENCE_ZIP_CODE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_ZIP_CODE_NUMBER, 
     last_value(CORRESPONDENCE_ZIP_CODE_EXT) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_ZIP_CODE_EXT, 
     last_value(CORRESPONDENCE_FULL_ZIP_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_FULL_ZIP_CODE, 
     last_value(CORRESPONDENCE_COUNTY_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CORRESPONDENCE_COUNTY_NAME, 
     last_value(NPI_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NPI_NUMBER, 
     last_value(NPI_CONFIRMATION_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as NPI_CONFIRMATION_IND, 
     last_value(MEDICARE_NUMBER) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MEDICARE_NUMBER, 
     last_value(MEDICARE_NUMBER_STATE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MEDICARE_NUMBER_STATE_CODE, 
     last_value(CURRENT_ENROLLMENT_STATUS) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_ENROLLMENT_STATUS, 
     last_value(CURRENT_CERTIFICATION_STATUS) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_CERTIFICATION_STATUS, 
     last_value(DEP_SERVICE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_SERVICE_CODE, 
     last_value(DEP_SERVICE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_SERVICE_DESC, 
     last_value(DEP_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_STATUS_DESC, 
     last_value(DEP_ACTIVE_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ACTIVE_IND, 
     last_value(DEP_90_DAY_EXAM_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_90_DAY_EXAM_IND, 
     last_value(DEP_ADR_IME_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ADR_IME_IND, 
     last_value(DEP_ADR_FILE_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ADR_FILE_REVIEW_IND, 
     last_value(DEP_B_READER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_B_READER_IND, 
     last_value(DEP_C92_EXAM_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_C92_EXAM_IND, 
     last_value(DEP_C92A_FILE_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_C92A_FILE_REVIEW_IND, 
     last_value(DEP_DUR_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_DUR_REVIEW_IND, 
     last_value(DEP_IME_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_IME_IND, 
     last_value(DEP_MEDICAL_FILE_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_MEDICAL_FILE_REVIEW_IND, 
     last_value(DEP_PRIOR_AUTHORIZATION_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_PRIOR_AUTHORIZATION_REVIEW_IND, 
     last_value(DEP_DM_IME_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_DM_IME_IND, 
     last_value(DEP_ADDITIONAL_LANGUAGE_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ADDITIONAL_LANGUAGE_IND, 
     last_value(DEP_ONLINE_FILE_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ONLINE_FILE_REVIEW_IND, 
     last_value(DEP_PULMONARY_EXAM_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_PULMONARY_EXAM_IND, 
     last_value(DEP_PULMONARY_REVIEW_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_PULMONARY_REVIEW_IND, 
     last_value(DEP_ASBESTOSIS_EXAM_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ASBESTOSIS_EXAM_IND, 
     last_value(MCO_MEDICAL_DIRECTOR_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MCO_MEDICAL_DIRECTOR_IND, 
     last_value(DEP_ADMINISTRATIVE_AGENT_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DEP_ADMINISTRATIVE_AGENT_NAME,
 PROVIDER_NAME, 
 PROVIDER_DBA_SORT_NAME, 
 MEDICARE_PROVIDER_TYPE_CODE, 
 MEDICARE_PROVIDER_TYPE_DESC, 
 CRITICAL_ACCESS_IND, 
 OPPS_QUALITY_IND, 
 PROVIDER_CBSA_CODE, 
 NEW_PATIENT_CODE, 
 NEW_PATIENT_DESC, 
 PROVIDER_PROGRAM_CODE, 
 PROVIDER_PROGRAM_DESC, 
 PROVIDER_PROGRAM_FOCUS_TEXT, 
 PROGRAM_PARTICIPATION_PERIOD
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
,
    CASE WHEN (ROW_NUMBER() OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM )) = 1 THEN '1901-01-01'::DATE
        WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
        WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
        else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE 
     
	FROM EDW_STAGING.DIM_PROVIDER_SCDALL_STEP2),

-------------- ETL LAYER -------
ETL AS (
select md5(cast(
    
    coalesce(cast(PROVIDER_PEACH_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) as  PROVIDER_HKEY
, UNIQUE_ID_KEY
, PROVIDER_PEACH_NUMBER
, PEACH_FORMATTED_NUMBER
, PEACH_BASE_NUMBER
, PEACH_SUFFIX_NUMBER
, PROVIDER_NAME
, PROVIDER_DBA_SORT_NAME
, PRACTICE_TYPE_DESC
, PROVIDER_TYPE_CODE
, PROVIDER_TYPE_DESC
, PROVIDER_SPECIALTY_LIST
, PROVIDER_PHONE_NUMBER
, OUT_OF_STATE_STATUS_DESC
, PRACTICE_STREET_ADDRESS_1
, PRACTICE_STREET_ADDRESS_2
, PRACTICE_ADDRESS_CITY
, PRACTICE_ADDRESS_STATE_CODE
, PRACTICE_ZIP_CODE_NUMBER
, PRACTICE_ZIP_CODE_EXT
, PRACTICE_FULL_ZIP_CODE
, PRACTICE_COUNTY_NAME
, CORRESPONDENCE_STREET_ADDRESS_1
, CORRESPONDENCE_STREET_ADDRESS_2
, CORRESPONDENCE_ADDRESS_CITY
, CORRESPONDENCE_ADDRESS_STATE_CODE
, CORRESPONDENCE_ZIP_CODE_NUMBER
, CORRESPONDENCE_ZIP_CODE_EXT
, CORRESPONDENCE_FULL_ZIP_CODE
, CORRESPONDENCE_COUNTY_NAME
, NPI_NUMBER
, NPI_CONFIRMATION_IND
, MEDICARE_NUMBER
, MEDICARE_NUMBER_STATE_CODE
, MEDICARE_PROVIDER_TYPE_CODE
, MEDICARE_PROVIDER_TYPE_DESC
, CRITICAL_ACCESS_IND
, OPPS_QUALITY_IND
, PROVIDER_CBSA_CODE
, CURRENT_ENROLLMENT_STATUS
, CURRENT_CERTIFICATION_STATUS
, NEW_PATIENT_CODE
, NEW_PATIENT_DESC
, PROVIDER_PROGRAM_CODE
, PROVIDER_PROGRAM_DESC
, PROVIDER_PROGRAM_FOCUS_TEXT
, PROGRAM_PARTICIPATION_PERIOD
, DEP_SERVICE_CODE
, DEP_SERVICE_DESC
, DEP_STATUS_DESC
, DEP_ACTIVE_IND
, DEP_90_DAY_EXAM_IND
, DEP_ADR_IME_IND
, DEP_ADR_FILE_REVIEW_IND
, DEP_B_READER_IND
, DEP_C92_EXAM_IND
, DEP_C92A_FILE_REVIEW_IND
, DEP_DUR_REVIEW_IND
, DEP_IME_IND
, DEP_MEDICAL_FILE_REVIEW_IND
, DEP_PRIOR_AUTHORIZATION_REVIEW_IND
, DEP_DM_IME_IND
, DEP_ADDITIONAL_LANGUAGE_IND
, DEP_ONLINE_FILE_REVIEW_IND
, DEP_PULMONARY_EXAM_IND
, DEP_PULMONARY_REVIEW_IND
, DEP_ASBESTOSIS_EXAM_IND
, MCO_MEDICAL_DIRECTOR_IND
, DEP_ADMINISTRATIVE_AGENT_NAME
, case when END_DATE is null then 'Y' else 'N' end as CURRENT_RECORD_IND
, EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE
, END_DATE AS RECORD_END_DATE
, EFFECTIVE_TIMESTAMP AS LOAD_DATETIME
, END_TIMESTAMP AS UPDATE_DATETIME
, 'PEACH' AS PRIMARY_SOURCE_SYSTEM
 from SCD)
SELECT * FROM ETL
      );
    