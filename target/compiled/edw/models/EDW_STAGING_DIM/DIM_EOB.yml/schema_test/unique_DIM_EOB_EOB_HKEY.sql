
    
    



select count(*) as validation_errors
from (

    select
        EOB_HKEY

    from EDW_STAGING_DIM.DIM_EOB
    where EOB_HKEY is not null
    group by EOB_HKEY
    having count(*) > 1

) validation_errors


