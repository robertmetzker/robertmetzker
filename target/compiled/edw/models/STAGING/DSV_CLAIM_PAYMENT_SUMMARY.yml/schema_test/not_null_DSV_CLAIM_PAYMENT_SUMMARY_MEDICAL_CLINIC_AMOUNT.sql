
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where MEDICAL_CLINIC_AMOUNT is null


