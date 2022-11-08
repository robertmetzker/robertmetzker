

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_BENEFIT_TYPE  as
      (

 WITH  SCD AS ( 
	SELECT md5(cast(
    
    coalesce(cast(BENEFIT_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(JURISDICTION_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(BENEFIT_REPORTING_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(INJURY_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY,
     last_value(BENEFIT_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BENEFIT_TYPE_CODE, 
     last_value(JURISDICTION_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as JURISDICTION_TYPE_CODE, 
     last_value(BENEFIT_REPORTING_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BENEFIT_REPORTING_TYPE_DESC, 
     last_value(BENEFIT_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BENEFIT_TYPE_DESC, 
     last_value(JURISDICTION_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as JURISDICTION_TYPE_DESC, 
     last_value(BENEFIT_CATEGORY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as BENEFIT_CATEGORY_TYPE_DESC, 
     last_value(INJURY_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_AWARD_INJURY_TYPE_CODE, 
     last_value(INJURY_TYPE_NAME) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SCHEDULE_AWARD_INJURY_TYPE_DESC
     
	FROM EDW_STAGING.DIM_BENEFIT_TYPE_SCDALL_STEP2)

select 
     md5(cast(
    
    coalesce(cast(BENEFIT_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(JURISDICTION_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(BENEFIT_REPORTING_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(SCHEDULE_AWARD_INJURY_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) AS BENEFIT_TYPE_HKEY
,UNIQUE_ID_KEY
,BENEFIT_TYPE_CODE
,JURISDICTION_TYPE_CODE
,BENEFIT_REPORTING_TYPE_DESC
,SCHEDULE_AWARD_INJURY_TYPE_CODE
,BENEFIT_TYPE_DESC
,JURISDICTION_TYPE_DESC
,BENEFIT_CATEGORY_TYPE_DESC
,SCHEDULE_AWARD_INJURY_TYPE_DESC
,CURRENT_TIMESTAMP AS  LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
 
 from SCD
      );
    