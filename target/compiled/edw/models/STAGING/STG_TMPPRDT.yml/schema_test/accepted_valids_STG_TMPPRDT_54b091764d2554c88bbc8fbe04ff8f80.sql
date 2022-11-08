



with all_values as(
  select distinct BSNS_TYPE_CODE as value_field
  from STAGING.STG_TMPPRDT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CORPT','INDIV','LLCCP','NONPF','PARTN','SCORP','SLPRO'
        )
)

select count(*)
from validation_errors

