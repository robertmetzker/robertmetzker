
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where TOTAL_MEDICAL_PAID_AMOUNT is null


