
    
    



select count(*) as validation_errors
from (

    select
        ACTV_DTL_ID

    from STAGING.STG_ACTIVITY_DETAIL
    where ACTV_DTL_ID is not null
    group by ACTV_DTL_ID
    having count(*) > 1

) validation_errors


