
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CUSTOMER
where CUSTOMER_HKEY is null


