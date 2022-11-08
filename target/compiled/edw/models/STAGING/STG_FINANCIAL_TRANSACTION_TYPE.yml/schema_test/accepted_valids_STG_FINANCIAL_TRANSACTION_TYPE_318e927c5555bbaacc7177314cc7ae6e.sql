



with all_values as(
  select distinct AGRE_TYP_NM as value_field
  from STAGING.STG_FINANCIAL_TRANSACTION_TYPE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CLAIM','CUSTOMER','POLICY','QUOTE'
        )
)

select count(*)
from validation_errors

