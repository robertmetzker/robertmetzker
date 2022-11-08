
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT is null


