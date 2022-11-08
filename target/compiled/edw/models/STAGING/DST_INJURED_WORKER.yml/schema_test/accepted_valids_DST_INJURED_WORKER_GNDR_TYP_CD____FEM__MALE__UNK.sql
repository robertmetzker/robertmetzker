



with all_values as(
  select distinct GNDR_TYP_CD as value_field
  from STAGING.DST_INJURED_WORKER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'FEM','MALE','UNK'
        )
)

select count(*)
from validation_errors

