
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_ISSUE
where CASE_ISS_ID is null


