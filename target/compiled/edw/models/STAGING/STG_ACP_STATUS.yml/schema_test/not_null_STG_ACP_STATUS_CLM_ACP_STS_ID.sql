
    
    



select count(*) as validation_errors
from STAGING.STG_ACP_STATUS
where CLM_ACP_STS_ID is null


