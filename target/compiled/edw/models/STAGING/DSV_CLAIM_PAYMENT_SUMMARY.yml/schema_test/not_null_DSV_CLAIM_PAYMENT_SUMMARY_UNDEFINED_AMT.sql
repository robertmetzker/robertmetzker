
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where UNDEFINED_AMT is null


