
    
    



select count(*) as validation_errors
from (

    select
        UNIQUE_ID_KEY

    from STAGING.DSV_ACTIVITY_DETAIL
    where UNIQUE_ID_KEY is not null
    group by UNIQUE_ID_KEY
    having count(*) > 1

) validation_errors


