



with all_values as(
  select distinct CD_EXM_REQS_TYP_CD as value_field
  from STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CLM_ASSTT','CSS','EMPLOYER','INDCOMIS','MCO','NURSE','TPA'
        )
)

select count(*)
from validation_errors

