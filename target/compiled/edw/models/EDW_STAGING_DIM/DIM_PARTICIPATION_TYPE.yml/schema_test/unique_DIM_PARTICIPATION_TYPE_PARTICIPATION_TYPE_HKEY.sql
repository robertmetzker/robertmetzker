
    
    



select count(*) as validation_errors
from (

    select
        PARTICIPATION_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_PARTICIPATION_TYPE
    where PARTICIPATION_TYPE_HKEY is not null
    group by PARTICIPATION_TYPE_HKEY
    having count(*) > 1

) validation_errors


