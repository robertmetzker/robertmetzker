 

WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(PROCEDURE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROCEDURE_CODE,
     last_value(PROCEDURE_CODE_ENTRY_DATE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROCEDURE_CODE_ENTRY_DATE,
     last_value(CPT_PAYMENT_CATEGORY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CPT_PAYMENT_CATEGORY,
     last_value(CPT_FEE_SCHEDULE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CPT_FEE_SCHEDULE_DESC,
     last_value(PROCEDURE_SERVICE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PROCEDURE_SERVICE_TYPE_DESC,
     last_value(CPT_PAYMENT_SUBCATEGORY) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CPT_PAYMENT_SUBCATEGORY,
 PROCEDURE_DESC,
 PROCEDURE_CODE_EFFECTIVE_DATE,
 PROCEDURE_CODE_END_DATE
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
-- ,
--     CASE WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
--       WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
--     else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CASE WHEN (ROW_NUMBER() OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM )) = 1 THEN PROCEDURE_CODE_EFFECTIVE_DATE::dATE 
        WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE 
     FROM EDW_STAGING.DIM_CPT_SCDALL_STEP2)

-- ETL LAYER --
 , ETL AS (select 
 md5(cast(
    
    coalesce(cast(PROCEDURE_CODE as 
    varchar
), '') || '-' || coalesce(cast(EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) as CPT_HKEY
, UNIQUE_ID_KEY
, PROCEDURE_CODE
, PROCEDURE_DESC
, PROCEDURE_CODE_ENTRY_DATE
, PROCEDURE_CODE_EFFECTIVE_DATE
, PROCEDURE_CODE_END_DATE
, PROCEDURE_SERVICE_TYPE_DESC
, CPT_PAYMENT_CATEGORY
, CPT_PAYMENT_SUBCATEGORY
, CPT_FEE_SCHEDULE_DESC
, CASE WHEN END_DATE IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_RECORD_IND
, EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE
, END_DATE AS RECORD_END_DATE
, EFFECTIVE_TIMESTAMP AS LOAD_DATETIME
, END_TIMESTAMP AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
 from SCD)

SELECT * FROM ETL