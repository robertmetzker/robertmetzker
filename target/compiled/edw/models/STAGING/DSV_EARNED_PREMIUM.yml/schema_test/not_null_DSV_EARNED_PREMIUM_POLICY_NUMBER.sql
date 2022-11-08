
    
    



select count(*) as validation_errors
from STAGING.DSV_EARNED_PREMIUM
where POLICY_NUMBER is null


