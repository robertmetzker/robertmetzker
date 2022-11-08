
    
    



select count(*) as validation_errors
from STAGING.DSV_POLICY_PERIOD
where UNIQUE_ID_KEY is null


