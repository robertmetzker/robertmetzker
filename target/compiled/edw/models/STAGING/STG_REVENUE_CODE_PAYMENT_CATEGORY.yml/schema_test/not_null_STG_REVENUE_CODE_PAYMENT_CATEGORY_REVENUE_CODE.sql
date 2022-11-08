
    
    



select count(*) as validation_errors
from STAGING.STG_REVENUE_CODE_PAYMENT_CATEGORY
where REVENUE_CODE is null


