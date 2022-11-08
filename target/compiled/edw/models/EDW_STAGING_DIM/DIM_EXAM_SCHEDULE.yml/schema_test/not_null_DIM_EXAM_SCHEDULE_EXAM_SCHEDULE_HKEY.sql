
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EXAM_SCHEDULE
where EXAM_SCHEDULE_HKEY is null


