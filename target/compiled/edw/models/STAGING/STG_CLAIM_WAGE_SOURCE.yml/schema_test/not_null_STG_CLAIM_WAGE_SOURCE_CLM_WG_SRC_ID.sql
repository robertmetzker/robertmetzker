
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_WAGE_SOURCE
where CLM_WG_SRC_ID is null

