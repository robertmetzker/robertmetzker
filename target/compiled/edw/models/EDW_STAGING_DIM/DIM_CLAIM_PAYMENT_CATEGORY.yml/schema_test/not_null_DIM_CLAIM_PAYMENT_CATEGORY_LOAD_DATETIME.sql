
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_PAYMENT_CATEGORY
where LOAD_DATETIME is null


