



with all_values as(
  select distinct CASE_VENU_LOC_TYP_NM as value_field
  from STAGING.STG_CASE_DETAIL_LEGAL 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AKRON','CAMBRIDGE','CINCINNATI','CLEVELAND','COLUMBUS','DAYTON','LIMA','LOGAN','MANSFIELD','PORTSMOUTH','TOLEDO','UNKNOWN','YOUNGSTOWN'
        )
)

select count(*)
from validation_errors

