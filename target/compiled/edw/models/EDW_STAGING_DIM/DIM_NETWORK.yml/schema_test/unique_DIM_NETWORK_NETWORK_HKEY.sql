
    
    



select count(*) as validation_errors
from (

    select
        NETWORK_HKEY

    from EDW_STAGING_DIM.DIM_NETWORK
    where NETWORK_HKEY is not null
    group by NETWORK_HKEY
    having count(*) > 1

) validation_errors


