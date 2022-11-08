



with all_values as(
  select distinct CDES_CLMT_AVL_SAT_IND as value_field
  from STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE 
  
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

