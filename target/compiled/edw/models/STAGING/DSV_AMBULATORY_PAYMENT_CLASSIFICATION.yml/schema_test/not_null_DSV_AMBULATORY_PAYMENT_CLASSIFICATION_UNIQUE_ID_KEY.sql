
    
    



select count(*) as validation_errors
from STAGING.DSV_AMBULATORY_PAYMENT_CLASSIFICATION
where UNIQUE_ID_KEY is null


