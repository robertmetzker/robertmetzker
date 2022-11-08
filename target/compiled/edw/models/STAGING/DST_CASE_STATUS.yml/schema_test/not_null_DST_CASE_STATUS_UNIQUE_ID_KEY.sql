
    
    



select count(*) as validation_errors
from STAGING.DST_CASE_STATUS
where UNIQUE_ID_KEY is null


