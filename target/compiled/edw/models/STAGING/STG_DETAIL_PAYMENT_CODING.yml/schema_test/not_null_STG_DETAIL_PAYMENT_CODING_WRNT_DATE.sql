
    
    



select count(*) as validation_errors
from STAGING.STG_DETAIL_PAYMENT_CODING
where WRNT_DATE is null


