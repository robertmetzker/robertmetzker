
    
    



select count(*) as validation_errors
from (

    select
        USER_HKEY

    from EDW_STAGING_DIM.DIM_USER
    where USER_HKEY is not null
    group by USER_HKEY
    having count(*) > 1

) validation_errors


