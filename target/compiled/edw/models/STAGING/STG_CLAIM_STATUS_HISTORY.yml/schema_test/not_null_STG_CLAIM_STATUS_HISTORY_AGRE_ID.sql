
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_STATUS_HISTORY
where AGRE_ID is null

