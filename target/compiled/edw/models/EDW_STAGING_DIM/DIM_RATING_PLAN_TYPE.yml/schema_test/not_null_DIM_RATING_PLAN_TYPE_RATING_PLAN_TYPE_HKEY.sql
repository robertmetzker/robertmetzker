
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_RATING_PLAN_TYPE
where RATING_PLAN_TYPE_HKEY is null


