
    
    



select count(*) as validation_errors
from STAGING.DSV_DETAIL_PAYMENT_CODING
where CHECK_NUMBER is null


