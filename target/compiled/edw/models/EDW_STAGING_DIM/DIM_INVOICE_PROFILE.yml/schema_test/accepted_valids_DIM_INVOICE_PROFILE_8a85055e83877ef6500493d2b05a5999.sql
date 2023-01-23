



with all_values as(
  select distinct MEDICAL_INVOICE_TYPE_CODE as value_field
  from EDW_STAGING_DIM.DIM_INVOICE_PROFILE 
  
      where INVOICE_PROFILE_HKEY not in ( MD5('99999'), MD5('-1111'), MD5('-2222'), MD5('88888'), MD5('00000') )
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'PR','PS','HO','HI','AS'
        )
)

select count(*)
from validation_errors
