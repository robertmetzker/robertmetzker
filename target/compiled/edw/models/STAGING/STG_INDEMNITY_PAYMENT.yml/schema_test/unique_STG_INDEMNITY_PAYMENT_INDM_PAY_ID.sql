
    
    



select count(*) as validation_errors
from (

    select
        INDM_PAY_ID

    from STAGING.STG_INDEMNITY_PAYMENT
    where INDM_PAY_ID is not null
    group by INDM_PAY_ID
    having count(*) > 1

) validation_errors


