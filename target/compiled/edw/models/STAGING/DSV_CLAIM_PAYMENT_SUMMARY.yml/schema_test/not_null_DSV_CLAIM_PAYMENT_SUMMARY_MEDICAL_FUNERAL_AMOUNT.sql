
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where MEDICAL_FUNERAL_AMOUNT is null


