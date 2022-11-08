
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where MEDICAL_OTHER_AMOUNT is null


