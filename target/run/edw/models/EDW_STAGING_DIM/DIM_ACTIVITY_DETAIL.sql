

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_ACTIVITY_DETAIL  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
          last_value(ACTIVITY_DETAIL_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ACTIVITY_DETAIL_DESC
   	FROM EDW_STAGING.DIM_ACTIVITY_DETAIL_SCDALL_STEP2),
ETL AS( 
SELECT
     md5(cast(
    
    coalesce(cast(ACTIVITY_DETAIL_DESC as 
    varchar
), '')

 as 
    varchar
)) As ACTIVITY_DETAIL_HKEY
    ,UNIQUE_ID_KEY
    ,ACTIVITY_DETAIL_DESC
    ,CURRENT_TIMESTAMP AS  LOAD_DATETIME
    ,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
    ,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
 from SCD
)

select * from ETL
      );
    