
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_PAYMENT_CATEGORY
where UNIQUE_ID_KEY is null


