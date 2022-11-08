
    
    



select count(*) as validation_errors
from (

    select
        EXAM_SCHEDULE_HKEY

    from EDW_STAGING_DIM.DIM_EXAM_SCHEDULE
    where EXAM_SCHEDULE_HKEY is not null
    group by EXAM_SCHEDULE_HKEY
    having count(*) > 1

) validation_errors


