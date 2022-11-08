
    
    



select count(*) as validation_errors
from STAGING.STG_APC_PAYMENT_ADJUSTMENT
where APC_PAYMENT_ADJUSTMENT_CODE is null


