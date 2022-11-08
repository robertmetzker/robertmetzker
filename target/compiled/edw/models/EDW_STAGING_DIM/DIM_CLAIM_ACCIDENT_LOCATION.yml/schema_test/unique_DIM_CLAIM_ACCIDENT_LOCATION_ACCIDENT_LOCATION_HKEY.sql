
    
    



select count(*) as validation_errors
from (

    select
        ACCIDENT_LOCATION_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_ACCIDENT_LOCATION
    where ACCIDENT_LOCATION_HKEY is not null
    group by ACCIDENT_LOCATION_HKEY
    having count(*) > 1

) validation_errors


