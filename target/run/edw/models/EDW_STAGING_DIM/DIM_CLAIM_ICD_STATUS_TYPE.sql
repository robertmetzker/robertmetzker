

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_CLAIM_ICD_STATUS_TYPE  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(ICD_STATUS_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ICD_STATUS_TYPE_CODE, 
     last_value(CLAIM_ICD_PRIMARY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_PRIMARY_IND, 
     last_value(ICD_STATUS_TYPE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ICD_STATUS_TYPE_NAME
     
	FROM EDW_STAGING.DIM_CLAIM_ICD_STATUS_TYPE_SCDALL_STEP2),

ETL AS (
select UNIQUE_ID_KEY AS CLAIM_ICD_STATUS_TYPE_HKEY 
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

SELECT * FROM ETL
      );
    