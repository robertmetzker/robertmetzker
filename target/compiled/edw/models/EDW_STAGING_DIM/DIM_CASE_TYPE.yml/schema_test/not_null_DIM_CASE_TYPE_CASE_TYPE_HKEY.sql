
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CASE_TYPE
where CASE_TYPE_HKEY is null


