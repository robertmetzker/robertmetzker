

      create or replace  table DEV_EDW.EDW_STAGING.DIM_CLAIM_TYPE_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT CLAIM_TYPE_DESC, CLAIM_TYPE_CODE , UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_CLAIM_TYPE )
select * from SCD1
      );
    