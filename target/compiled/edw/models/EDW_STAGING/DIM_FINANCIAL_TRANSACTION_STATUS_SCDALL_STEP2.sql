----SRC LAYER----
WITH
SCD1 as ( SELECT FINANCIAL_TRANSACTION_STATUS_DESC, FINANCIAL_TRANSACTION_STATUS_CODE , UNIQUE_ID_KEY    
	--, '1901-01-01' as DBT_VALID_FROM, '2099-12-31' as DBT_VALID_TO
	from      STAGING.DSV_FINANCIAL_TRANSACTION_STATUS )

select * from SCD1