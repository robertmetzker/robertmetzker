



with all_values as(
  select distinct CLM_TYP_CD as value_field
  from STAGING.STG_CLAIM_TYPE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LOSTTM','MEDONLY','ICDN'
        )
)

select count(*)
from validation_errors
