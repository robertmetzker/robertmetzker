
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_AMBULATORY_PAYMENT_CLASSIFICATION
where APC_HKEY is null


