
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PAYMENT_CODER
where LOAD_DATETIME is null


