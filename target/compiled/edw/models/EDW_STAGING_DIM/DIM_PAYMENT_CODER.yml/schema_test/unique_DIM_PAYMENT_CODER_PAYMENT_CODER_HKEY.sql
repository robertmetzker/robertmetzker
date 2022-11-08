
    
    



select count(*) as validation_errors
from (

    select
        PAYMENT_CODER_HKEY

    from EDW_STAGING_DIM.DIM_PAYMENT_CODER
    where PAYMENT_CODER_HKEY is not null
    group by PAYMENT_CODER_HKEY
    having count(*) > 1

) validation_errors


