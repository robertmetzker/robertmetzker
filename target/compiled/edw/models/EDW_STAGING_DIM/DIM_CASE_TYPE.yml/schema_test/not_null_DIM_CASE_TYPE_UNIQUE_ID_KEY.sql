
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CASE_TYPE
where UNIQUE_ID_KEY is null


