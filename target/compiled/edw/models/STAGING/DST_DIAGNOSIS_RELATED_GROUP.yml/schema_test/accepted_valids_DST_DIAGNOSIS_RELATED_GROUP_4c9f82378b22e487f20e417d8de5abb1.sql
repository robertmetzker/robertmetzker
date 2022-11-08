



with all_values as(
  select distinct PA_TYPE_DESC as value_field
  from STAGING.DST_DIAGNOSIS_RELATED_GROUP 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'POST-ACUTE CARE TRANSFER','SPECIAL POST-ACUTE CARE TRANSFER'
        )
)

select count(*)
from validation_errors

