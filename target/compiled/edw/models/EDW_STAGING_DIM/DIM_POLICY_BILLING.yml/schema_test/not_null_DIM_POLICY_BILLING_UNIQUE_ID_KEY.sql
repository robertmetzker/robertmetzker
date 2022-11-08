
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_BILLING
where UNIQUE_ID_KEY is null


