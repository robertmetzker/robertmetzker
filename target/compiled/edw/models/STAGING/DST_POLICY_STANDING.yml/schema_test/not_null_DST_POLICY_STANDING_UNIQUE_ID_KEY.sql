
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_STANDING
where UNIQUE_ID_KEY is null


