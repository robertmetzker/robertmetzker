
    
    



select count(*) as validation_errors
from (

    select
        PAYEE_HKEY

    from EDW_STAGING_DIM.DIM_PAYEE
    where PAYEE_HKEY is not null
    group by PAYEE_HKEY
    having count(*) > 1

) validation_errors


