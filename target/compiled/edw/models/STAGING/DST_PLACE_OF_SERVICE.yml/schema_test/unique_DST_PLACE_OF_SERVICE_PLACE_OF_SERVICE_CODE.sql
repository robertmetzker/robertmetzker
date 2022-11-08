
    
    



select count(*) as validation_errors
from (

    select
        PLACE_OF_SERVICE_CODE

    from STAGING.DST_PLACE_OF_SERVICE
    where PLACE_OF_SERVICE_CODE is not null
    group by PLACE_OF_SERVICE_CODE
    having count(*) > 1

) validation_errors


