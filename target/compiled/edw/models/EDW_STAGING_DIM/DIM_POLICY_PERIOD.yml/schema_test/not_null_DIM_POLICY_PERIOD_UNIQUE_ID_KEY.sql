
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_PERIOD
where UNIQUE_ID_KEY is null


