





with validation_errors as (

    select
        ACTIVITY_DETAIL_DESC
    from EDW_STAGING_DIM.DIM_ACTIVITY_DETAIL

    group by ACTIVITY_DETAIL_DESC
    having count(*) > 1

)

select count(*)
from validation_errors


