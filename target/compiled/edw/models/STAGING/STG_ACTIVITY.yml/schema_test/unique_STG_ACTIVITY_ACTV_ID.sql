
    
    



select count(*) as validation_errors
from (

    select
        ACTV_ID

    from STAGING.STG_ACTIVITY
    where ACTV_ID is not null
    group by ACTV_ID
    having count(*) > 1

) validation_errors


