
    
    



select count(*) as validation_errors
from (

    select
        PLACE_OF_SERVICE_HKEY

    from EDW_STAGING_DIM.DIM_PLACE_OF_SERVICE
    where PLACE_OF_SERVICE_HKEY is not null
    group by PLACE_OF_SERVICE_HKEY
    having count(*) > 1

) validation_errors


