
    
    



select count(*) as validation_errors
from STAGING.DSV_EARNED_PREMIUM
where BILLED_DATE is null


