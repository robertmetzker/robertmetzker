

      create or replace  table DEV_EDW.EDW_STAGING.DIM_RATING_PLAN_TYPE_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT RATING_PLAN_CODE, RATING_PLAN_DESC , UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_RATING_PLAN_TYPE )
select * from SCD1
      );
    