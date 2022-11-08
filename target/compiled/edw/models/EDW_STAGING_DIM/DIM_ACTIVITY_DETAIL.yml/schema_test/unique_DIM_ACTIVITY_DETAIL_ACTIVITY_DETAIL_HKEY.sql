
    
    



select count(*) as validation_errors
from (

    select
        ACTIVITY_DETAIL_HKEY

    from EDW_STAGING_DIM.DIM_ACTIVITY_DETAIL
    where ACTIVITY_DETAIL_HKEY is not null
    group by ACTIVITY_DETAIL_HKEY
    having count(*) > 1

) validation_errors


