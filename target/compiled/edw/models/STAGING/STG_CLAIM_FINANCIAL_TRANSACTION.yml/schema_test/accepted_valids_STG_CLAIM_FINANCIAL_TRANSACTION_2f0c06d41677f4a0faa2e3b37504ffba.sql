



with all_values as(
  select distinct FNCL_TRAN_TYP_ACCT_PYBL_IND as value_field
  from STAGING.STG_CLAIM_FINANCIAL_TRANSACTION 
  
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

