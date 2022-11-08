



with all_values as(
  select distinct CASE_SERV_STS_TYP_CD as value_field
  from STAGING.STG_CASE_SERVICE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'APRV','DNYD','PND'
        )
)

select count(*)
from validation_errors

