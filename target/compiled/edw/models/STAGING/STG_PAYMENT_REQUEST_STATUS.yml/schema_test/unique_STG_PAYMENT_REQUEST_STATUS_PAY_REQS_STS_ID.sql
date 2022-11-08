
    
    



select count(*) as validation_errors
from (

    select
        PAY_REQS_STS_ID

    from STAGING.STG_PAYMENT_REQUEST_STATUS
    where PAY_REQS_STS_ID is not null
    group by PAY_REQS_STS_ID
    having count(*) > 1

) validation_errors


