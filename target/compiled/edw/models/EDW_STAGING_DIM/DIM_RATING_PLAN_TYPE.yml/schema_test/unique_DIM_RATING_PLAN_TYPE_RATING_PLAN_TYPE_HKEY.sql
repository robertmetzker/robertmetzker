
    
    



select count(*) as validation_errors
from (

    select
        RATING_PLAN_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_RATING_PLAN_TYPE
    where RATING_PLAN_TYPE_HKEY is not null
    group by RATING_PLAN_TYPE_HKEY
    having count(*) > 1

) validation_errors


