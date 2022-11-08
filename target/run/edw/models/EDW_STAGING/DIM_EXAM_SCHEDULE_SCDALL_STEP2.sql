

      create or replace  table DEV_EDW.EDW_STAGING.DIM_EXAM_SCHEDULE_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT 
THURSDAY_AVAILABILITY_IND, 
TUESDAY_AVAILABILITY_IND, 
ADDENDUM_REQUESTED_IND, 
TRAVEL_REIMBURSEMENT_IND, 
ADDITIONAL_TESTING_IND, 
SATURDAY_AVAILABILITY_IND, 
FRIDAY_AVAILABILITY_IND, 
MONDAY_AVAILABILITY_IND, 
WEDNESDAY_AVAILABILITY_IND, 
INTERPRETER_NEEDED_IND, 
SUNDAY_AVAILABILITY_IND, 
RESULT_SUSPENDED_IND, 
GREATER_THAN_45_MILES_IND , 
UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_EXAM_SCHEDULE )
	
	select * from SCD1
      );
    