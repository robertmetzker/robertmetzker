



with all_values as(
  select distinct BNFT_CYC_TYP_CD as value_field
  from STAGING.STG_INDEMNITY_PAYMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'FXD','PRD'
        )
)

select count(*)
from validation_errors

