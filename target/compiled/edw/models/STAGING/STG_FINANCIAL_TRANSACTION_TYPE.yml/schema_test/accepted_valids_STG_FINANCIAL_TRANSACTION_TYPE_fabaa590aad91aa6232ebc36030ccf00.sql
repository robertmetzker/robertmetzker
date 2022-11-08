



with all_values as(
  select distinct FNCL_TRAN_TYP_GNRL_LDGR_IND as value_field
  from STAGING.STG_FINANCIAL_TRANSACTION_TYPE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

