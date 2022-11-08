



with all_values as(
  select distinct CDMM_CERT_OTPTNT_SERV_IND as value_field
  from STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT 
  
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

