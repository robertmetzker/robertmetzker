
    
    



select count(*) as validation_errors
from (

    select
        DISABILITY_MANAGEMENT_ID

    from STAGING.DSV_DISABILITY_MANAGEMENT
    where DISABILITY_MANAGEMENT_ID is not null
    group by DISABILITY_MANAGEMENT_ID
    having count(*) > 1

) validation_errors


