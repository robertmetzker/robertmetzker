



with all_values as(
  select distinct LEASE_TYPE_CODE as value_field
  from EDW_STAGING_DIM.DIM_YEAR_CONTROL_ELEMENT 
  
      where YEAR_CONTROL_ELEMENT_HKEY not in ( MD5('99999'), MD5('-1111'), MD5('-2222'), MD5('88888'), MD5('00000') )
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'LS_PLCY_NON_PEO','LS_PLCY_TYP_PEO','LS_TYP_AEO_CLNT','LS_TYP_PEO_CLNT','LS_PLCY_TYP_AEO'
        )
)

select count(*)
from validation_errors

