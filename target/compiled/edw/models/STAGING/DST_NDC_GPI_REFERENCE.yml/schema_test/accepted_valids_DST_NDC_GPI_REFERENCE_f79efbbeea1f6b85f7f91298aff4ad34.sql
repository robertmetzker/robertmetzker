



with all_values as(
  select distinct DRUG_TYPE_DESC as value_field
  from STAGING.DST_NDC_GPI_REFERENCE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'NATIONAL DRUG CODE'
        )
)

select count(*)
from validation_errors

