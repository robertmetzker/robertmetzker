
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where CONTRACT_DOCTOR_AMOUNT is null


