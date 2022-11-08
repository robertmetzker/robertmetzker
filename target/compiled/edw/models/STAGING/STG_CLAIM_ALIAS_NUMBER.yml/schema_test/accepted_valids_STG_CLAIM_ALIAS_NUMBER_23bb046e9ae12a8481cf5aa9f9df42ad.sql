



with all_values as(
  select distinct CLM_ALIAS_NO_TYP_CD as value_field
  from STAGING.STG_CLAIM_ALIAS_NUMBER 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'DEPLBRCASENO','DUPEXPRDCLM','ODJFSUNEMPNUM','OSHACASENO','SLFINSEMPLRID'
        )
)

select count(*)
from validation_errors

