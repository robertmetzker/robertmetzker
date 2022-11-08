

 WITH  SCD AS ( 
	SELECT  UNIQUE_ID_KEY,
          last_value(MONDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as MONDAY_AVAILABILITY_IND, 
     last_value(TUESDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as TUESDAY_AVAILABILITY_IND, 
     last_value(WEDNESDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as WEDNESDAY_AVAILABILITY_IND, 
     last_value(THURSDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as THURSDAY_AVAILABILITY_IND, 
     last_value(FRIDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as FRIDAY_AVAILABILITY_IND, 
     last_value(SATURDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SATURDAY_AVAILABILITY_IND, 
     last_value(SUNDAY_AVAILABILITY_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as SUNDAY_AVAILABILITY_IND, 
     last_value(INTERPRETER_NEEDED_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as INTERPRETER_NEEDED_IND, 
     last_value(GREATER_THAN_45_MILES_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as GREATER_THAN_45_MILES_IND, 
     last_value(TRAVEL_REIMBURSEMENT_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as TRAVEL_REIMBURSEMENT_IND, 
     last_value(ADDITIONAL_TESTING_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ADDITIONAL_TESTING_IND, 
     last_value(ADDENDUM_REQUESTED_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as ADDENDUM_REQUESTED_IND, 
     last_value(RESULT_SUSPENDED_IND) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as RESULT_SUSPENDED_IND
    
	FROM EDW_STAGING.DIM_EXAM_SCHEDULE_SCDALL_STEP2),
    
----------ETL LAYER-------------------
ETL AS( 
SELECT
     md5(cast(
    
    coalesce(cast(MONDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(TUESDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(WEDNESDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(THURSDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(FRIDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(SATURDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(SUNDAY_AVAILABILITY_IND as 
    varchar
), '') || '-' || coalesce(cast(INTERPRETER_NEEDED_IND as 
    varchar
), '') || '-' || coalesce(cast(GREATER_THAN_45_MILES_IND as 
    varchar
), '') || '-' || coalesce(cast(TRAVEL_REIMBURSEMENT_IND as 
    varchar
), '') || '-' || coalesce(cast(ADDITIONAL_TESTING_IND as 
    varchar
), '') || '-' || coalesce(cast(ADDENDUM_REQUESTED_IND as 
    varchar
), '') || '-' || coalesce(cast(RESULT_SUSPENDED_IND as 
    varchar
), '')

 as 
    varchar
)) As EXAM_SCHEDULE_HKEY
,UNIQUE_ID_KEY
,MONDAY_AVAILABILITY_IND
,TUESDAY_AVAILABILITY_IND
,WEDNESDAY_AVAILABILITY_IND
,THURSDAY_AVAILABILITY_IND
,FRIDAY_AVAILABILITY_IND
,SATURDAY_AVAILABILITY_IND
,SUNDAY_AVAILABILITY_IND
,INTERPRETER_NEEDED_IND
,GREATER_THAN_45_MILES_IND
,TRAVEL_REIMBURSEMENT_IND
,ADDITIONAL_TESTING_IND
,ADDENDUM_REQUESTED_IND
,RESULT_SUSPENDED_IND
,CURRENT_TIMESTAMP AS  LOAD_DATETIME
,TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
,'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
 from SCD
)

select * from ETL