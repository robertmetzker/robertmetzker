



with all_values as(
  select distinct ADRS_TYPE_CODE as value_field
  from STAGING.STG_PROVIDER_CONTACT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CORRE','HOME','PRCBU','RMBRS','T1099'
        )
)

select count(*)
from validation_errors

