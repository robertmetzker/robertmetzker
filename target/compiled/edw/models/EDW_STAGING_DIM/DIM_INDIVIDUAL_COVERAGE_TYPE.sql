

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(INDIVIDUAL_COVERAGE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDIVIDUAL_COVERAGE_TYPE_DESC, 
     last_value(INDIVIDUAL_COVERAGE_TITLE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDIVIDUAL_COVERAGE_TITLE, 
     last_value(INDIVIDUAL_COVERAGE_INCLUSION_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INDIVIDUAL_COVERAGE_INCLUSION_IND
     
	FROM EDW_STAGING.DIM_INDIVIDUAL_COVERAGE_TYPE_SCDALL_STEP2),

-------------ETL LAYER---------

ETL AS (
select md5(cast(
    
    coalesce(cast(INDIVIDUAL_COVERAGE_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(INDIVIDUAL_COVERAGE_TITLE as 
    varchar
), '') || '-' || coalesce(cast(INDIVIDUAL_COVERAGE_INCLUSION_IND as 
    varchar
), '')

 as 
    varchar
)) AS INDIVIDUAL_COVERAGE_TYPE_HKEY
    , * 
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'CAM' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

SELECT * FROM ETL