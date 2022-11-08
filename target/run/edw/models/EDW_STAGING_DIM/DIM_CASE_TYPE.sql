

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_CASE_TYPE  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(CONTEXT_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CONTEXT_TYPE_CODE, 
     last_value(CASE_CATEGORY_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_CATEGORY_TYPE_CODE, 
     last_value(CASE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_TYPE_CODE, 
     last_value(CASE_PRIORITY_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_PRIORITY_TYPE_CODE, 
     last_value(CASE_RESOLUTION_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_RESOLUTION_TYPE_CODE, 
     last_value(CONTEXT_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CONTEXT_TYPE_DESC, 
     last_value(CASE_CATEGORY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_CATEGORY_TYPE_DESC, 
     last_value(CASE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_TYPE_DESC, 
     last_value(CASE_PRIORITY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_PRIORITY_TYPE_DESC, 
     last_value(CASE_RESOLUTION_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CASE_RESOLUTION_TYPE_DESC

			FROM EDW_STAGING.DIM_CASE_TYPE_SCDALL_STEP2),

ETL AS (
select 
 CASE WHEN nullif(array_to_string(array_construct_compact( CONTEXT_TYPE_CODE, CASE_CATEGORY_TYPE_CODE, CASE_TYPE_CODE, CASE_PRIORITY_TYPE_CODE, CASE_RESOLUTION_TYPE_CODE),''), '') is NULL  
THEN MD5( '99999' ) ELSE  
md5(cast(
    
    coalesce(cast(CONTEXT_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CASE_CATEGORY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CASE_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CASE_PRIORITY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CASE_RESOLUTION_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) 
 end AS CASE_TYPE_HKEY
--   UNIQUE_ID_KEY AS CASE_TYPE_HKEY 
, CONTEXT_TYPE_CODE
, CASE_CATEGORY_TYPE_CODE
, CASE_TYPE_CODE
, CASE_PRIORITY_TYPE_CODE
, CASE_RESOLUTION_TYPE_CODE
, CONTEXT_TYPE_DESC
, CASE_CATEGORY_TYPE_DESC
, CASE_TYPE_DESC
, CASE_PRIORITY_TYPE_DESC
, CASE_RESOLUTION_TYPE_DESC
, CURRENT_TIMESTAMP AS  LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 

from SCD
)

SELECT * FROM ETL
      );
    