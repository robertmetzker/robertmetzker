

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(INDUSTRY_GROUP_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDUSTRY_GROUP_CODE, 
 INDUSTRY_GROUP_DESC
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 
,CASE WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,CAST(DBT_VALID_TO AS DATE) as END_DATE 
,CASE WHEN DBT_VALID_TO IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_INDICATOR
     
	FROM EDW_STAGING.DIM_INDUSTRY_GROUP_SCDALL_STEP2),

-------------ETL LAYER---------
ETL AS(SELECT
 UNIQUE_ID_KEY::CHAR(32) AS INDUSTRY_GROUP_HKEY
, UNIQUE_ID_KEY
, INDUSTRY_GROUP_CODE
, INDUSTRY_GROUP_DESC
, CURRENT_INDICATOR as CURRENT_RECORD_IND
, EFFECTIVE_DATE as RECORD_EFFECTIVE_DATE
, END_DATE as RECORD_END_DATE
,CURRENT_TIMESTAMP AS  LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
FROM SCD)

SELECT * FROM ETL