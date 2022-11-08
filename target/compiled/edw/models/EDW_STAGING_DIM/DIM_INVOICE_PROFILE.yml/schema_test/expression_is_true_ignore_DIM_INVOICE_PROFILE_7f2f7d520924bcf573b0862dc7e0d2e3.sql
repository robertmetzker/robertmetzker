





with meet_condition as (

    select * from EDW_STAGING_DIM.DIM_INVOICE_PROFILE  
    where INVOICE_PROFILE_HKEY not in ( MD5('99999'), MD5('-1111'), MD5('-2222'), MD5('88888'), MD5('00000') )
    
),
validation_errors as (

    select
        *
    from meet_condition
    
    where not(LENGTH(MEDICAL_INVOICE_TYPE_CODE) = 2)

)

select count(*)
from validation_errors

