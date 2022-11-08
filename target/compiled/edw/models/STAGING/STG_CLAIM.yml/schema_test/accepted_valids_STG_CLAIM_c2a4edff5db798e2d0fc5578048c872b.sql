



with all_values as(
  select distinct OCCR_MEDA_TYP_CD as value_field
  from STAGING.STG_CLAIM 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'DOCM','EDI','EMAIL','FAX','MEDREPO','TELPHNCALL','WALKIN','WEBRPT'
        )
)

select count(*)
from validation_errors

