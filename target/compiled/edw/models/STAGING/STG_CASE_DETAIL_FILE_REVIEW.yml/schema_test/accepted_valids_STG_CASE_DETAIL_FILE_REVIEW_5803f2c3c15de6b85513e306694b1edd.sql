



with all_values as(
  select distinct CD_ADNDM_REQS_TYP_NM as value_field
  from STAGING.STG_CASE_DETAIL_FILE_REVIEW 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CARRIER RESPONSIBILITY','PHYSICIAN RESPONSIBILITY'
        )
)

select count(*)
from validation_errors

