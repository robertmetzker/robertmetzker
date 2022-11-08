
    
    



select count(*) as validation_errors
from (

    select
        INJURED_WORKER_HKEY

    from EDW_STAGING_DIM.DIM_INJURED_WORKER
    where INJURED_WORKER_HKEY is not null
    group by INJURED_WORKER_HKEY
    having count(*) > 1

) validation_errors


