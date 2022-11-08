
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_STANDING
where UNIQUE_ID_KEY is null


