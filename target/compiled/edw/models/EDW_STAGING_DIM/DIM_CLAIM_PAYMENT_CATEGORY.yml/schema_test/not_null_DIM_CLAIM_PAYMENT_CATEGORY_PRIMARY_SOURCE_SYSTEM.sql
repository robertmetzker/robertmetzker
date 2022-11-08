
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_PAYMENT_CATEGORY
where PRIMARY_SOURCE_SYSTEM is null


