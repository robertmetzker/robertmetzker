
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_RATING_PLAN_TYPE
where LOAD_DATETIME is null


