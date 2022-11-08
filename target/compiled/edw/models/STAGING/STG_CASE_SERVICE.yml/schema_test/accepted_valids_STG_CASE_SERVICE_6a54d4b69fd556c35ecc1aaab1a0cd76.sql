



with all_values as(
  select distinct CASE_SERV_TYP_CD as value_field
  from STAGING.STG_CASE_SERVICE 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'CPT','DGPI','HCPC','HM_VN_MOD','ICD9','ICD10','MISCELLANEOUS','SI_PRS','TH_DRG_GRP','TOOTH'
        )
)

select count(*)
from validation_errors

