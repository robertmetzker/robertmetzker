
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_STATUS_HISTORY
where CASE_STT_STS_ID is null


