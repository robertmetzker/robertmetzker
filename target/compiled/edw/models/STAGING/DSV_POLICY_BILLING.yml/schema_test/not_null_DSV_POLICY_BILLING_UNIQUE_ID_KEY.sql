
    
    



select count(*) as validation_errors
from STAGING.DSV_POLICY_BILLING
where UNIQUE_ID_KEY is null


