



with all_values as(
  select distinct OCCR_MEDA_TYP_NM as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'MAIL','EDI','EMAIL','FAX','MEDICAL REPOSITORY','PHONE','WALK IN','WEB REPORT'
        )
)

select count(*)
from validation_errors

