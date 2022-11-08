
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_ACTIVITY
where CLM_AGRE_ID is null


