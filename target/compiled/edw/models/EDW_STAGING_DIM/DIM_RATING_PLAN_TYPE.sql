

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(RATING_PLAN_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RATING_PLAN_CODE, 
     last_value(RATING_PLAN_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RATING_PLAN_DESC
	FROM EDW_STAGING.DIM_RATING_PLAN_TYPE_SCDALL_STEP2)
---------ETL LAYER--------
,ETL AS( 
SELECT
    md5(cast(
    
    coalesce(cast(RATING_PLAN_CODE as 
    varchar
), '')

 as 
    varchar
)) AS RATING_PLAN_TYPE_HKEY
    ,UNIQUE_ID_KEY
    ,RATING_PLAN_CODE
    ,RATING_PLAN_DESC
    ,CURRENT_TIMESTAMP AS  LOAD_DATETIME
    ,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    ,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
 from SCD
)

select * from ETL