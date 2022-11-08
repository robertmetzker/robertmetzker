
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_PARTICIPATION_INSURED
where UNIQUE_ID_KEY is null


