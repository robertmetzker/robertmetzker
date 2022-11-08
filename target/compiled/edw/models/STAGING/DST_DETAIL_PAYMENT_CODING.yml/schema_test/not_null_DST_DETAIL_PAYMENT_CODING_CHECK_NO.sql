
    
    



select count(*) as validation_errors
from STAGING.DST_DETAIL_PAYMENT_CODING
where CHECK_NO is null


