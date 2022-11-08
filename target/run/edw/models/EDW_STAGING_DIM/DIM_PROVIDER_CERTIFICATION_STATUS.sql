

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_PROVIDER_CERTIFICATION_STATUS  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(BWC_CERTIFICATION_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_CERTIFICATION_STATUS_CODE, 
     last_value(BWC_CERTIFICATION_STATUS_REASON_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_CERTIFICATION_STATUS_REASON_CODE, 
     last_value(BWC_CERTIFICATION_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_CERTIFICATION_STATUS_DESC, 
     last_value(BWC_CERTIFICATION_STATUS_REASON_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BWC_CERTIFICATION_STATUS_REASON_DESC
	FROM EDW_STAGING.DIM_PROVIDER_CERTIFICATION_STATUS_SCDALL_STEP2),

    ETL AS (
select UNIQUE_ID_KEY AS BWC_CERTIFICATION_STATUS_HKEY
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'PEACH' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)
 SELECT * FROM ETL
      );
    