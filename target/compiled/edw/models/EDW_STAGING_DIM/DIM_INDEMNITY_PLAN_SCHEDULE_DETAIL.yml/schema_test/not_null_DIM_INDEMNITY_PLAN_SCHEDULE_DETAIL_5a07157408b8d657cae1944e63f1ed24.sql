
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INDEMNITY_PLAN_SCHEDULE_DETAIL
where INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY is null


