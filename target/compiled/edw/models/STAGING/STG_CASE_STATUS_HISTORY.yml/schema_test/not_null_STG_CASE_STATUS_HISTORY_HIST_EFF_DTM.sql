
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_STATUS_HISTORY
where HIST_EFF_DTM is null


