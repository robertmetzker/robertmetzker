
    
    



select count(*) as validation_errors
from STAGING.STG_PAYMENT_REQUEST_STATUS
where PAY_REQS_STS_ID is null


