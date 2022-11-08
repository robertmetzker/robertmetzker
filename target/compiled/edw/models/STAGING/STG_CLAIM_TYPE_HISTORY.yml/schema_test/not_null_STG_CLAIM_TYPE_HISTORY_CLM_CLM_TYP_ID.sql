
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_TYPE_HISTORY
where CLM_CLM_TYP_ID is null


