
    
    



select count(*) as validation_errors
from STAGING.STG_PAYMENT_REQUEST
where PAY_REQS_ID is null


