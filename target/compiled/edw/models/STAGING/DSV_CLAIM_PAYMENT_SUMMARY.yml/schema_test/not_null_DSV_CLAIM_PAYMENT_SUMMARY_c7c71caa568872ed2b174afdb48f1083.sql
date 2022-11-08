
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT is null


