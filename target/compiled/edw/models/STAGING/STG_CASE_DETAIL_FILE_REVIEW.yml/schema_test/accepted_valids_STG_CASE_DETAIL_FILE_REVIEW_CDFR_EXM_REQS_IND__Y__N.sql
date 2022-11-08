



with all_values as(
  select distinct CDFR_EXM_REQS_IND as value_field
  from STAGING.STG_CASE_DETAIL_FILE_REVIEW 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'Y','N'
        )
)

select count(*)
from validation_errors

