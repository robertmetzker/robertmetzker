

      create or replace  table DEV_EDW.EDW_STAGING.DIM_RATING_ELEMENT_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT RATE_ELEMENT_TYPE_CODE, RATE_ELEMENT_TYPE_DESC, RATE_ELEMENT_USAGE_DESC, RATE_ELEMENT_USAGE_CODE , UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_RATING_ELEMENT )

select * from SCD1
      );
    