
    
    



select count(*) as validation_errors
from STAGING.DST_AMBULATORY_PAYMENT_CLASSIFICATION
where APC_CODE is null


