
    
    



select count(*) as validation_errors
from STAGING.DSV_DETAIL_PAYMENT_CODING
where TCN_NUMBER is null


