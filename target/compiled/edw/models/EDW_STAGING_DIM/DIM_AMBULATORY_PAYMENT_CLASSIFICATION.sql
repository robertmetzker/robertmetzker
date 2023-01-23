

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(APC_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by dbt_updated_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as APC_CODE,
 APC_DESC, 
 APC_CODE_STATUS_CODE, 
 APC_CODE_STATUS_DESC, 
 APC_RELATIVE_WEIGHT_RATE, 
 APC_AMOUNT, 
 APC_EFFECTIVE_DATE, 
 APC_EXPIRATION_DATE
,DBT_VALID_FROM AS EFFECTIVE_TIMESTAMP 
,DBT_VALID_TO   AS END_TIMESTAMP 

, CASE WHEN (ROW_NUMBER() OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY DBT_VALID_FROM )) = 1 THEN APC_EFFECTIVE_DATE 
        WHEN CAST(DBT_VALID_FROM AS DATE) = '1901-01-01' then CAST(DBT_VALID_FROM AS DATE)
      WHEN CAST(DBT_VALID_FROM AS DATE) <> '1901-01-01' THEN dateadd(day,1,CAST(DBT_VALID_FROM AS DATE))
    else CAST(DBT_VALID_FROM AS DATE) end as EFFECTIVE_DATE
,
    CAST(DBT_VALID_TO AS DATE) as END_DATE 
,CASE WHEN DBT_VALID_TO IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_INDICATOR
     
	FROM EDW_STAGING.DIM_AMBULATORY_PAYMENT_CLASSIFICATION_SCDALL_STEP2)
,
------ETL LAYER--------
ETL1 as ( SELECT
md5(cast(
    
    coalesce(cast(APC_CODE as 
    varchar
), '') || '-' || coalesce(cast(EFFECTIVE_DATE as 
    varchar
), '')

 as 
    varchar
)) AS APC_HKEY
, UNIQUE_ID_KEY
, APC_CODE
, APC_DESC
, APC_CODE_STATUS_CODE
, APC_CODE_STATUS_DESC
, CAST(APC_RELATIVE_WEIGHT_RATE AS NUMERIC(32,2)) AS APC_RELATIVE_WEIGHT_RATE
, CAST(APC_AMOUNT AS NUMERIC(32,2)) AS APC_AMOUNT
, APC_EFFECTIVE_DATE
, APC_EXPIRATION_DATE
, CASE WHEN END_DATE IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_RECORD_IND
, EFFECTIVE_DATE AS RECORD_EFFECTIVE_DATE
, END_DATE AS RECORD_END_DATE
, EFFECTIVE_TIMESTAMP AS LOAD_DATETIME
, END_TIMESTAMP AS UPDATE_DATETIME
, 'CAM' AS PRIMARY_SOURCE_SYSTEM
 from SCD)

SELECT * FROM ETL1