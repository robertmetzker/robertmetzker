
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_PERIOD
where UNIQUE_ID_KEY is null


