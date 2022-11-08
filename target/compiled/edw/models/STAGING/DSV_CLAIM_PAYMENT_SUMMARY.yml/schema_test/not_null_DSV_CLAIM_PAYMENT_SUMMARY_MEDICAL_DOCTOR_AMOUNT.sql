
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where MEDICAL_DOCTOR_AMOUNT is null


