
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PAYMENT_CODER
where PAYMENT_CODER_HKEY is null


