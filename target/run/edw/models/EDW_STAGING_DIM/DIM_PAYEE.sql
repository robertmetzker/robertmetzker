

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_PAYEE  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
        last_value(PAYEE_FULL_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PAYEE_FULL_NAME
	FROM EDW_STAGING.DIM_PAYEE_SCDALL_STEP2),
    ETL AS (
select md5(cast(
    
    coalesce(cast(PAYEE_FULL_NAME as 
    varchar
), '')

 as 
    varchar
)) AS PAYEE_HKEY
    ,UNIQUE_ID_KEY
    ,PAYEE_FULL_NAME
    , CURRENT_TIMESTAMP AS  LOAD_DATETIME
    , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    , 'RNP' AS PRIMARY_SOURCE_SYSTEM 
    from SCD
)

SELECT * FROM ETL
      );
    