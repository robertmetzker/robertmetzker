

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_CLAIM_ICD_STATUS_DETAIL  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(CLAIM_ICD_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_STATUS_CODE, 
     last_value(CLAIM_ICD_LOCATION_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_LOCATION_CODE, 
     last_value(CLAIM_ICD_SITE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_SITE_CODE, 
     last_value(CLAIM_ICD_PRIMARY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_PRIMARY_IND, 
     last_value(CURRENT_ICD_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CURRENT_ICD_IND, 
     last_value(CLAIM_ICD_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_STATUS_DESC, 
     last_value(CLAIM_ICD_SITE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_SITE_DESC
    
	FROM EDW_STAGING.DIM_CLAIM_ICD_STATUS_DETAIL_SCDALL_STEP2)

--- ETL Layer --------
,
ETL AS (
select 
     UNIQUE_ID_KEY AS CLAIM_ICD_STATUS_DETAIL_HKEY 
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

SELECT * FROM ETL
      );
    