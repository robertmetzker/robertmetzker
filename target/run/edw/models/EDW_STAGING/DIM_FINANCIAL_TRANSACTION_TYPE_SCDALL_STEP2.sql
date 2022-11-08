

      create or replace  table DEV_EDW.EDW_STAGING.DIM_FINANCIAL_TRANSACTION_TYPE_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT FINANCIAL_TRANSACTION_TYPE_DESC, AGREEMENT_TYPE_DESC, FINANCIAL_TRANSACTION_TYPE_CODE, FINANCIAL_TRANSACTION_TYPE_ID, UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_FINANCIAL_TRANSACTION_TYPE )

select * from SCD1
      );
    