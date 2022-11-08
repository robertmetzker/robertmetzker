
    
    



select count(*) as validation_errors
from STAGING.DSV_AMBULATORY_PAYMENT_CLASSIFICATION
where APC_CODE is null


