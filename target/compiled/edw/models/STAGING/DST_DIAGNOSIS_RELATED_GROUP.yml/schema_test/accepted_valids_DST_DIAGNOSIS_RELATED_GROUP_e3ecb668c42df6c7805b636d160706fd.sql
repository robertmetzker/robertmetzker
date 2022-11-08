



with all_values as(
  select distinct DRG_TYPE_DESC as value_field
  from STAGING.DST_DIAGNOSIS_RELATED_GROUP 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MEDICAL DRG','SURGICAL DRG'
        )
)

select count(*)
from validation_errors

