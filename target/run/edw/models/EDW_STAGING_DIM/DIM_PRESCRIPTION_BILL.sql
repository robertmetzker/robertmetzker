

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_PRESCRIPTION_BILL  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     
     last_value(SERVICE_LEVEL_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SERVICE_LEVEL_CODE,
     last_value(PHARMACIST_SERVICE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHARMACIST_SERVICE_CODE,
     last_value(SERVICE_REASON_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SERVICE_REASON_CODE,
     last_value(SERVICE_RESULT_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SERVICE_RESULT_CODE,
     last_value(SUBMITTED_DAW_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUBMITTED_DAW_CODE,
     last_value(SUBMISSION_CLARIFICATION_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUBMISSION_CLARIFICATION_CODE,
     last_value(PBM_BENEFIT_PLAN_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PBM_BENEFIT_PLAN_TYPE_DESC,
     last_value(PBM_ORIGINATION_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PBM_ORIGINATION_TYPE_DESC,
     last_value(PHARM_SPECIAL_PROGRAM_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHARM_SPECIAL_PROGRAM_DESC,
     last_value(PBM_LOCK_IN_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PBM_LOCK_IN_IND,
     last_value(DRUG_REFILL_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DRUG_REFILL_IND,
     last_value(SERVICE_LEVEL_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SERVICE_LEVEL_DESC,
     last_value(PHARMACIST_SERVICE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PHARMACIST_SERVICE_DESC,
     last_value(SERVICE_REASON_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SERVICE_REASON_DESC,
     last_value(SERVICE_RESULT_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SERVICE_RESULT_DESC,
     last_value(SUBMITTED_DAW_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUBMITTED_DAW_DESC,
     last_value(SUBMISSION_CLARIFICATION_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUBMISSION_CLARIFICATION_DESC

     FROM EDW_STAGING.DIM_PRESCRIPTION_BILL_SCDALL_STEP2),
ETL AS (select md5(cast(
    
    coalesce(cast(SERVICE_LEVEL_CODE as 
    varchar
), '') || '-' || coalesce(cast(PHARMACIST_SERVICE_CODE as 
    varchar
), '') || '-' || coalesce(cast(SERVICE_REASON_CODE as 
    varchar
), '') || '-' || coalesce(cast(SERVICE_RESULT_CODE as 
    varchar
), '') || '-' || coalesce(cast(SUBMITTED_DAW_CODE as 
    varchar
), '') || '-' || coalesce(cast(SUBMISSION_CLARIFICATION_CODE as 
    varchar
), '') || '-' || coalesce(cast(PBM_BENEFIT_PLAN_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(PBM_ORIGINATION_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(PHARM_SPECIAL_PROGRAM_DESC as 
    varchar
), '') || '-' || coalesce(cast(PBM_LOCK_IN_IND as 
    varchar
), '') || '-' || coalesce(cast(DRUG_REFILL_IND as 
    varchar
), '')

 as 
    varchar
)) as PRESCRIPTION_HKEY
,UNIQUE_ID_KEY
,SERVICE_LEVEL_CODE
,PHARMACIST_SERVICE_CODE
,SERVICE_REASON_CODE
,SERVICE_RESULT_CODE
,SUBMITTED_DAW_CODE
,SUBMISSION_CLARIFICATION_CODE
,PBM_BENEFIT_PLAN_TYPE_DESC
,PBM_ORIGINATION_TYPE_DESC
,PHARM_SPECIAL_PROGRAM_DESC
,PBM_LOCK_IN_IND
,DRUG_REFILL_IND
,SERVICE_LEVEL_DESC
,PHARMACIST_SERVICE_DESC
,SERVICE_REASON_DESC
,SERVICE_RESULT_DESC
,SUBMITTED_DAW_DESC
,SUBMISSION_CLARIFICATION_DESC
,CURRENT_TIMESTAMP() AS LOAD_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
 from SCD)
 select * from ETL
      );
    