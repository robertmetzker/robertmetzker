
    
    



select count(*) as validation_errors
from (

    select
        ACTIVITY_HKEY

    from EDW_STAGING_DIM.DIM_ACTIVITY
    where ACTIVITY_HKEY is not null
    group by ACTIVITY_HKEY
    having count(*) > 1

) validation_errors


