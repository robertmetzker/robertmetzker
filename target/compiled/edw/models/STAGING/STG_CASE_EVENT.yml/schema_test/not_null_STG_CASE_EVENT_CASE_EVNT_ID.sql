
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_EVENT
where CASE_EVNT_ID is null


