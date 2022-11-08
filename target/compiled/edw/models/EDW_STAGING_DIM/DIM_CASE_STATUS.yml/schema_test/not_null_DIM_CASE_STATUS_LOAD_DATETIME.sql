
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CASE_STATUS
where LOAD_DATETIME is null


