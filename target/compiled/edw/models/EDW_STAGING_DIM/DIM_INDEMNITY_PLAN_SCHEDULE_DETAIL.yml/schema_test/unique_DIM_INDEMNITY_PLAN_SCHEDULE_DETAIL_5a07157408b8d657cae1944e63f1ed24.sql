
    
    



select count(*) as validation_errors
from (

    select
        INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY

    from EDW_STAGING_DIM.DIM_INDEMNITY_PLAN_SCHEDULE_DETAIL
    where INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY is not null
    group by INDEMNITY_PLAN_SCHEDULE_DETAIL_HKEY
    having count(*) > 1

) validation_errors


