

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_CLAIM_PAYMENT_CATEGORY  as
      (
/*
 WITH  SCD AS ( 
	SELECT  
     last_value(CLAIM_PAYMENT_CATEGORY_HKEY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_PAYMENT_CATEGORY_HKEY, 
     last_value(CLAIM_PAYMENT_CATEGORY_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_PAYMENT_CATEGORY_DESC, 
     last_value(CLAIM_PAYMENT_CATEGORY_TYPE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_PAYMENT_CATEGORY_TYPE
     
	FROM EDW_STAGING.DIM_CLAIM_PAYMENT_CATEGORY_SCDALL_STEP2)
*/
--- ETL Layer --------
WITH ETL AS (select DISTINCT
         md5(cast(
    
    coalesce(cast(CLAIM_PAYMENT_CATEGORY_DESC as 
    varchar
), '')

 as 
    varchar
)) AS CLAIM_PAYMENT_CATEGORY_HKEY
       , md5(cast(
    
    coalesce(cast(CLAIM_PAYMENT_CATEGORY_DESC as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
       , CLAIM_PAYMENT_CATEGORY_DESC
       , CASE WHEN startswith(CLAIM_PAYMENT_CATEGORY_DESC, 'INDEMNITY') THEN 'INDEMNITY'
              WHEN startswith(CLAIM_PAYMENT_CATEGORY_DESC,  'MEDICAL') THEN 'MEDICAL'
              WHEN startswith(CLAIM_PAYMENT_CATEGORY_DESC, 'UNDEFINED')  THEN 'UNDEFINED'
              ELSE 'OTHER' END AS CLAIM_PAYMENT_CATEGORY_TYPE 
       , CURRENT_TIMESTAMP AS LOAD_DATETIME
       , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
       , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
       from EDW_STAGING.DIM_CLAIM_PAYMENT_CATEGORY_SCDALL_STEP2)
 SELECT * FROM ETL
      );
    