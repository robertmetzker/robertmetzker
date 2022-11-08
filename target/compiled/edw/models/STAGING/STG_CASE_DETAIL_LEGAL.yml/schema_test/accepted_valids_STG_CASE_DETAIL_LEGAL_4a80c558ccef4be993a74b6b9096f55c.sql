



with all_values as(
  select distinct CASE_VENU_LOC_TYP_IC_CD as value_field
  from STAGING.STG_CASE_DETAIL_LEGAL 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'AKR','CAM','CIN','CLE','COL','DAY','LIM','LOG','MAN','POR','TOL','UNK','YOU'
        )
)

select count(*)
from validation_errors

