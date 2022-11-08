
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_RESERVE
where CLM_RSRV_ID is null


