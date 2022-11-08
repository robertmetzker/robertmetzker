
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT is null


