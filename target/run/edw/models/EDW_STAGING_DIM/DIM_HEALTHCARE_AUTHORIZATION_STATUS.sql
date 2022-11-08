

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_HEALTHCARE_AUTHORIZATION_STATUS  as
      ( 

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,      
     last_value(AUTHORIZATION_SERVICE_VOID_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AUTHORIZATION_SERVICE_VOID_IND, 
     last_value(AUTHORIZATION_STATUS_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AUTHORIZATION_STATUS_CODE, 
     last_value(AUTHORIZATION_SERVICE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AUTHORIZATION_SERVICE_TYPE_CODE, 
     last_value(AUTHORIZATION_STATUS_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AUTHORIZATION_STATUS_DESC, 
     last_value(AUTHORIZATION_SERVICE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as AUTHORIZATION_SERVICE_TYPE_DESC
        
	FROM EDW_STAGING.DIM_HEALTHCARE_AUTHORIZATION_STATUS_SCDALL_STEP2)
, ETL AS (
select  
  UNIQUE_ID_KEY as HEALTHCARE_AUTHORIZATION_STATUS_HKEY
, UNIQUE_ID_KEY
, AUTHORIZATION_STATUS_CODE
, AUTHORIZATION_SERVICE_TYPE_CODE
, AUTHORIZATION_SERVICE_VOID_IND
, AUTHORIZATION_STATUS_DESC
, AUTHORIZATION_SERVICE_TYPE_DESC
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CORESUITE' as PRIMARY_SOURCE_SYSTEM
from SCD
)
SELECT * FROM ETL
      );
    