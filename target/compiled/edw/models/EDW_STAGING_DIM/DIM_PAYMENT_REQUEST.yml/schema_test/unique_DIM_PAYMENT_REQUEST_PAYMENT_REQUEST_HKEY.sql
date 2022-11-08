
    
    



select count(*) as validation_errors
from (

    select
        PAYMENT_REQUEST_HKEY

    from EDW_STAGING_DIM.DIM_PAYMENT_REQUEST
    where PAYMENT_REQUEST_HKEY is not null
    group by PAYMENT_REQUEST_HKEY
    having count(*) > 1

) validation_errors


