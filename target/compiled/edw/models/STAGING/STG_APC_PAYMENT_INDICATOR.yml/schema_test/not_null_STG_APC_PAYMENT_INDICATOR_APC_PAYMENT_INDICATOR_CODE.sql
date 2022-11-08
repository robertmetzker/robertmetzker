
    
    



select count(*) as validation_errors
from STAGING.STG_APC_PAYMENT_INDICATOR
where APC_PAYMENT_INDICATOR_CODE is null


