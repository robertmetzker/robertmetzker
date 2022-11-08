
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_NUMBER

    from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
    where CLAIM_NUMBER is not null
    group by CLAIM_NUMBER
    having count(*) > 1

) validation_errors


