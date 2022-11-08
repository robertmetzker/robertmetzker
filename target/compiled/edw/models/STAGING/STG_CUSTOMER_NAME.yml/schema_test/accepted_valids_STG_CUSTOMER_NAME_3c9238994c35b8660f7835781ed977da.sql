



with all_values as(
  select distinct CUST_NM_TYP_NM as value_field
  from STAGING.STG_CUSTOMER_NAME 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'BUSINESS ALIAS','BUSINESS LEGAL NAME','PAYEE NAME','PRIMARY DBA NAME','PERSON ALIAS','PERSON NAME','SECONDARY DBA NAME'
        )
)

select count(*)
from validation_errors

