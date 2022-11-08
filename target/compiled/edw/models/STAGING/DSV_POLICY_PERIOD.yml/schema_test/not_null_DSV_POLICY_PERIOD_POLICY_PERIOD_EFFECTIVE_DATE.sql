
    
    



select count(*) as validation_errors
from STAGING.DSV_POLICY_PERIOD
where POLICY_PERIOD_EFFECTIVE_DATE is null


