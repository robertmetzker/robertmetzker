
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PAYMENT_REQUEST
where LOAD_DATETIME is null


