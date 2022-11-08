

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_EDIT  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(EDIT_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EDIT_CODE, 
     last_value(APPLIED_BY_DESCRIPTION) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APPLIED_BY_DESCRIPTION,
 EDIT_SHORT_DESCRIPTION, 
 EDIT_LONG_DESCRIPTION, 
 EDIT_CATEGORY_CODE, 
 EDIT_CATEGORY_DESCRIPTION, 
 EDIT_EFFECTIVE_DATE, 
 EDIT_END_DATE, 
 DATE_RANGE_DESC
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
,
    CASE WHEN (ROW_NUMBER() OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM )) = 1 THEN EDIT_EFFECTIVE_DATE 
        WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE 
,CASE WHEN DBT_VALID_TO IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_INDICATOR
     
	FROM EDW_STAGING.DIM_EDIT_SCDALL_STEP2)

--- ETL LAYER ----
, 
ETL AS ( Select 
md5(cast(
    
    coalesce(cast(EDIT_CODE as 
    varchar
), '') || '-' || coalesce(cast(EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) as EDIT_HKEY
, UNIQUE_ID_KEY
, EDIT_CODE
, EDIT_SHORT_DESCRIPTION
, EDIT_LONG_DESCRIPTION
, EDIT_CATEGORY_CODE
, EDIT_CATEGORY_DESCRIPTION
, EDIT_EFFECTIVE_DATE
, EDIT_END_DATE
, DATE_RANGE_DESC
, APPLIED_BY_DESCRIPTION
, case when END_DATE is null then 'Y' else 'N' end as CURRENT_RECORD_IND
, EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE
, END_DATE AS RECORD_END_DATE
, EFFECTIVE_TIMESTAMP AS LOAD_DATETIME
, END_TIMESTAMP AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
from SCD)
SELECT * FROM ETL
      );
    