
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_BILLING
where UNIQUE_ID_KEY is null


