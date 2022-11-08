
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_DETAIL_PAYMENT_CODING
where UNIQUE_ID_KEY is null


