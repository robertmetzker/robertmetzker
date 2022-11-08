
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INDEMNITY_PLAN_SCHEDULE_DETAIL
where PRIMARY_SOURCE_SYSTEM is null


