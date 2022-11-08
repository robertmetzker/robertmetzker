
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_POLICY_HISTORY
where CLM_AGRE_ID is null


