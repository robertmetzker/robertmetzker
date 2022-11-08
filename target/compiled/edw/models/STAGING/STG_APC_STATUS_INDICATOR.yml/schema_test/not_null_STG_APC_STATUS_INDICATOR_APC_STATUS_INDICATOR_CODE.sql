
    
    



select count(*) as validation_errors
from STAGING.STG_APC_STATUS_INDICATOR
where APC_STATUS_INDICATOR_CODE is null


