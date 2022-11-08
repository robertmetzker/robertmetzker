
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where TOTAL_CLAIM_PAYMENT_AMOUNT is null


