
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where INDEMNITY_DEATH_BENEFIT_AMOUNT is null


