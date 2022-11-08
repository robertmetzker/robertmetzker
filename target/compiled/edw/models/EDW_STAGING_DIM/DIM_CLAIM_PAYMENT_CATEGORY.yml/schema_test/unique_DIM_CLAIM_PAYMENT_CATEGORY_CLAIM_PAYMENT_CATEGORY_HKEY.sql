
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_PAYMENT_CATEGORY_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_PAYMENT_CATEGORY
    where CLAIM_PAYMENT_CATEGORY_HKEY is not null
    group by CLAIM_PAYMENT_CATEGORY_HKEY
    having count(*) > 1

) validation_errors


