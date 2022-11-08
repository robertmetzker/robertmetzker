
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_STATUS_HISTORY
where CASE_STATUS_HISTORY_ID is null


