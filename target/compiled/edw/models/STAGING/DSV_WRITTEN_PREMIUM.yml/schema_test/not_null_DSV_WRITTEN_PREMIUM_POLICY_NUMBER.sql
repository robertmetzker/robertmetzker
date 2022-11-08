
    
    



select count(*) as validation_errors
from STAGING.DSV_WRITTEN_PREMIUM
where POLICY_NUMBER is null


