 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(CLAIM_STATE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_STATE_CODE,
     last_value(CLAIM_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_STATUS_CODE,
     last_value(CLAIM_STATUS_REASON_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_STATUS_REASON_CODE,
     last_value(CLAIM_STATE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_STATE_DESC,
     last_value(CLAIM_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_STATUS_DESC,
     last_value(CLAIM_STATUS_REASON_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_STATUS_REASON_DESC

     FROM EDW_STAGING.DIM_CLAIM_STATUS_SCDALL_STEP2),

--- ETL Layer --------
ETL AS (select 
md5(cast(
    
    coalesce(cast(CLAIM_STATE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_REASON_CODE as 
    varchar
), '')

 as 
    varchar
)) AS CLAIM_STATUS_HKEY
,UNIQUE_ID_KEY
,CLAIM_STATE_CODE
,CLAIM_STATUS_CODE
,CLAIM_STATUS_REASON_CODE
,CLAIM_STATE_DESC
,CLAIM_STATUS_DESC
,CLAIM_STATUS_REASON_DESC
,CURRENT_TIMESTAMP AS LOAD_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
 from SCD)
 SELECT * FROM ETL