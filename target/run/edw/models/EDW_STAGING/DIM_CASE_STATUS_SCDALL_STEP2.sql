

      create or replace  table DEV_EDW.EDW_STAGING.DIM_CASE_STATUS_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT 
CASE_STATUS_REASON_CODE, 
CASE_STATE_DESC, 
CASE_STATE_CODE, 
CASE_STATUS_REASON_DESC, 
CASE_STATUS_DESC, 
CASE_STATUS_CODE , 
UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_CASE_STATUS )
select * from SCD1
      );
    