
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where ATTORNEY_FEE_AMOUNT is null


