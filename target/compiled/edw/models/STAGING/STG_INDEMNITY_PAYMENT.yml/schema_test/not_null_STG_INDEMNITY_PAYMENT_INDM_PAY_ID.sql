
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_PAYMENT
where INDM_PAY_ID is null


