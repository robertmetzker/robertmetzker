



with all_values as(
  select distinct CASE_CTG_TYP_CD as value_field
  from STAGING.STG_CASES 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LGL','MED','VOC_RHBL','INVSTG','CMPLT','SI_AUDT','SI_CMPLT','CASE_MGMT','FLD_SERV','MED_MANG','SRGT'
        )
)

select count(*)
from validation_errors

