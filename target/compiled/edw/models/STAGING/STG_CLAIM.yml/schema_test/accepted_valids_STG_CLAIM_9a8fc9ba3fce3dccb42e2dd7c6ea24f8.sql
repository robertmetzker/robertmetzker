



with all_values as(
  select distinct OCCR_PRMS_TYP_CD as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'EMPLR','JOBSITE','LESSEE','OTHR'
        )
)

select count(*)
from validation_errors

