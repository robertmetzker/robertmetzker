



with all_values as(
  select distinct DRUG_TYPE_CODE as value_field
  from STAGING.DST_NDC_GPI_REFERENCE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        '03'
        )
)

select count(*)
from validation_errors

