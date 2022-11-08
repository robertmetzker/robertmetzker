
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_PROFILE_HKEY

    from EDW_STAGING_DIM.DIM_INVOICE_PROFILE
    where INVOICE_PROFILE_HKEY is not null
    group by INVOICE_PROFILE_HKEY
    having count(*) > 1

) validation_errors


