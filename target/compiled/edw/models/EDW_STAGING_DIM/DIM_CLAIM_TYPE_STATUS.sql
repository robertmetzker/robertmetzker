

--- Based on Requirement from DA, CTE has been hardcoded with 'Y', 'N' values for CROSS JOIN
WITH 
--SRC_CLAIM_TYPE_CHNG_IND as (select 'Y' as CLAIM_TYPE_CHANGE_OVER_IND UNION SELECT 'N' as CLAIM_TYPE_CHANGE_OVER_IND ),

SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(CLAIM_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_TYPE_CODE, 
     last_value(CLAIM_TYPE_CHANGE_OVER_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_TYPE_CHANGE_OVER_IND,
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
     last_value(CLAIM_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_TYPE_DESC, 
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
            ) as CLAIM_STATUS_REASON_DESC, 
     last_value(CLAIM_WEB_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_WEB_STATUS_CODE, 
     last_value(CLAIM_WEB_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_WEB_STATUS_DESC
     
	FROM EDW_STAGING.DIM_CLAIM_TYPE_STATUS_SCDALL_STEP2), 


------ ETL LAYER --------------

ETL AS (SELECT 
    md5(cast(
    
    coalesce(cast(CLAIM_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_TYPE_CHANGE_OVER_IND as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_REASON_CODE as 
    varchar
), '')

 as 
    varchar
)) AS CLAIM_TYPE_STATUS_HKEY
    --  UNIQUE_ID_KEY AS CLAIM_TYPE_STATUS_HKEY
    , UNIQUE_ID_KEY 
    , CLAIM_TYPE_CODE
    , CLAIM_TYPE_CHANGE_OVER_IND
    , CLAIM_STATE_CODE
    , CLAIM_STATUS_CODE
    , CLAIM_STATUS_REASON_CODE
    , CLAIM_TYPE_DESC
    , CLAIM_STATE_DESC
    , CLAIM_STATUS_DESC
    , CLAIM_STATUS_REASON_DESC
    , CLAIM_WEB_STATUS_CODE
    , CLAIM_WEB_STATUS_DESC
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
        from SCD
           )

SELECT * FROM ETL