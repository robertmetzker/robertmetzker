

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_EDIT_EOB_ENTRY  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(SOURCE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SOURCE_CODE, 
     last_value(ADJUDICATION_PHASE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ADJUDICATION_PHASE, 
     last_value(DISPOSITION_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DISPOSITION_CODE, 
     last_value(DISPOSITION_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as DISPOSITION_DESC
     
	FROM EDW_STAGING.DIM_EDIT_EOB_ENTRY_SCDALL_STEP2)

select 
md5(cast(
    
    coalesce(cast(SOURCE_CODE as 
    varchar
), '') || '-' || coalesce(cast(ADJUDICATION_PHASE as 
    varchar
), '') || '-' || coalesce(cast(DISPOSITION_CODE as 
    varchar
), '')

 as 
    varchar
)) as EDIT_EOB_ENTRY_HKEY 
, UNIQUE_ID_KEY
, SOURCE_CODE
, ADJUDICATION_PHASE
, DISPOSITION_CODE
, CASE WHEN LEFT(SOURCE_CODE,1) IN ('A', '7', '8') THEN 'BWC-MBAA' 
        WHEN SOURCE_CODE IN ('SYSTEM', 'CARE', 'DBYRNE') THEN 'CAM' 
        WHEN SOURCE_CODE ='MCO' THEN 'MANAGED CARE ORGANIZATION'
        ELSE 'UNKNOWN' END AS SOURCE_NAME
, DISPOSITION_DESC
, CURRENT_TIMESTAMP AS LOAD_DATETIME
,try_to_TIMESTAMP('Invalid')  AS UPDATE_DATETIME
,'CAM' AS PRIMARY_SOURCE_SYSTEM
 from SCD
      );
    