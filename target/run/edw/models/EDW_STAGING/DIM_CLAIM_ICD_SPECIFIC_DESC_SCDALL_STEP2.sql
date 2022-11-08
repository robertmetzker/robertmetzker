

      create or replace  table DEV_EDW.EDW_STAGING.DIM_CLAIM_ICD_SPECIFIC_DESC_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT  CLM_ICD_DESC , UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_CLAIM_ICD_STATUS_HISTORY )

select * from SCD1
      );
    