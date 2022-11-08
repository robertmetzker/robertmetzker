



with all_values as(
  select distinct GENDER_SPECIFIC_CODE as value_field
  from EDW_STAGING_DIM.DIM_ICD 
  
      where ICD_HKEY not in ( MD5('99999'), MD5('-1111'), MD5('-2222'), MD5('88888'), MD5('00000') )
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'M','F','B'
        )
)

select count(*)
from validation_errors

