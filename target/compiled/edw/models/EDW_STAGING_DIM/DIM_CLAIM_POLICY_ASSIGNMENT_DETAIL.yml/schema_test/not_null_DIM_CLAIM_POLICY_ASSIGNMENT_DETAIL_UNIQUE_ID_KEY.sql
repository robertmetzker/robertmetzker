
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_POLICY_ASSIGNMENT_DETAIL
where UNIQUE_ID_KEY is null


