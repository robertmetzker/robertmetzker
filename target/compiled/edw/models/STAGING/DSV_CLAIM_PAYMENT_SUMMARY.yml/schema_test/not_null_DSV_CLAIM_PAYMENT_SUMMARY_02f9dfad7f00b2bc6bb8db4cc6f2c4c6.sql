
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where INDEMNITY_TEMPORARY_PARTIAL_AMOUNT is null


