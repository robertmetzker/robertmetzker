
    
    



select count(*) as validation_errors
from STAGING.STG_CPT_PAYMENT_CATEGORY
where PAYMENT_CATEGORY is null


