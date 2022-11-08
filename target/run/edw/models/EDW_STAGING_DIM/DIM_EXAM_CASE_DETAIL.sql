

      create or replace  table DEV_EDW.EDW_STAGING_DIM.DIM_EXAM_CASE_DETAIL  as
      (

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
     last_value(EXAM_REQUEST_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EXAM_REQUEST_TYPE_CODE, 
     last_value(PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE, 
     last_value(SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE, 
     last_value(ADDENDUM_REQUEST_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ADDENDUM_REQUEST_TYPE_CODE, 
     last_value(LANGUAGE_TYPE_CODE) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as LANGUAGE_TYPE_CODE, 
     last_value(EXAM_REQUEST_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as EXAM_REQUEST_TYPE_DESC, 
     last_value(PRIMARY_PROVIDER_SPEIALTY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as PRIMARY_PROVIDER_SPEIALTY_TYPE_DESC, 
     last_value(SECONDARY_PROVIDER_SPEIALTY_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SECONDARY_PROVIDER_SPEIALTY_TYPE_DESC, 
     last_value(ADDENDUM_REQUEST_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ADDENDUM_REQUEST_TYPE_DESC, 
     last_value(LANGUAGE_TYPE_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as LANGUAGE_TYPE_DESC

	FROM EDW_STAGING.DIM_EXAM_CASE_DETAIL_SCDALL_STEP2),
 ----------ETL LAYER-------------------
ETL AS( 
SELECT
 UNIQUE_ID_KEY  AS EXAM_CASE_DETAIL_HKEY
,UNIQUE_ID_KEY 
,EXAM_REQUEST_TYPE_CODE
,PRIMARY_PROVIDER_SPECIALTY_TYPE_CODE
,SECONDARY_PROVIDER_SPECIALTY_TYPE_CODE
,ADDENDUM_REQUEST_TYPE_CODE
,LANGUAGE_TYPE_CODE
,EXAM_REQUEST_TYPE_DESC
,PRIMARY_PROVIDER_SPEIALTY_TYPE_DESC
,SECONDARY_PROVIDER_SPEIALTY_TYPE_DESC
,ADDENDUM_REQUEST_TYPE_DESC
,LANGUAGE_TYPE_DESC
,CURRENT_TIMESTAMP AS  LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
 from SCD
)

select * from ETL
      );
    