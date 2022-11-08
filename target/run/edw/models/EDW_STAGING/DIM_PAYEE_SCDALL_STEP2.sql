

      create or replace  table DEV_EDW.EDW_STAGING.DIM_PAYEE_SCDALL_STEP2  as
      (
 ----SRC LAYER----
WITH
SCD1 as ( SELECT PAYEE_FULL_NAME, UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_PAYEE_NAME )
select * from SCD1
      );
    