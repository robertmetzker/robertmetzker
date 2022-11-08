
    
    



select count(*) as validation_errors
from STAGING.DSV_DETAIL_PAYMENT_CODING
where WARRANT_NUMBER is null


