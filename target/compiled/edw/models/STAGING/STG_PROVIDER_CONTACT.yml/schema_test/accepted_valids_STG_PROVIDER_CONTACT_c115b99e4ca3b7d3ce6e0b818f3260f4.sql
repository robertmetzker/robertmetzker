



with all_values as(
  select distinct CNTCT_PRP_TYP_CODE as value_field
  from STAGING.STG_PROVIDER_CONTACT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'APPC','BILLN','ENRL','OFFC','OFMGR','PRYMN'
        )
)

select count(*)
from validation_errors

