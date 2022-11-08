
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PAYMENT_CODER
where PRIMARY_SOURCE_SYSTEM is null


