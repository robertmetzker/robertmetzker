
    
    



select count(*) as validation_errors
from (

    select
        PAY_REQS_ID

    from STAGING.STG_PAYMENT_REQUEST
    where PAY_REQS_ID is not null
    group by PAY_REQS_ID
    having count(*) > 1

) validation_errors


