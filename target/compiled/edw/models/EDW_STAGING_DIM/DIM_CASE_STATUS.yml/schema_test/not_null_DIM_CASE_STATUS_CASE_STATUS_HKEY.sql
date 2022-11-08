
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CASE_STATUS
where CASE_STATUS_HKEY is null


