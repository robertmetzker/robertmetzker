
    
    



select count(*) as validation_errors
from STAGING.STG_OPPS_PAYMENT_METHOD
where OPPS_PAYMENT_METHOD_CODE is null


