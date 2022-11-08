
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_POLICY_ASSIGNMENT_DETAIL
where CURRENT_CLAIM_POLICY_IND is null


