
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_PAYMENT_CATEGORY
where CLAIM_PAYMENT_CATEGORY_HKEY is null


