
    
    



select count(*) as validation_errors
from (

    select
        APC_HKEY

    from EDW_STAGING_DIM.DIM_AMBULATORY_PAYMENT_CLASSIFICATION
    where APC_HKEY is not null
    group by APC_HKEY
    having count(*) > 1

) validation_errors


