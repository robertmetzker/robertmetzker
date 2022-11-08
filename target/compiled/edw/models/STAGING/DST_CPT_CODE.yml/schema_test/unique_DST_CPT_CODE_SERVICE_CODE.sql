
    
    



select count(*) as validation_errors
from (

    select
        SERVICE_CODE

    from STAGING.DST_CPT_CODE
    where SERVICE_CODE is not null
    group by SERVICE_CODE
    having count(*) > 1

) validation_errors


