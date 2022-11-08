
    
    



select count(*) as validation_errors
from STAGING.DST_AMBULATORY_PAYMENT_CLASSIFICATION
where UNIQUE_ID_KEY is null


