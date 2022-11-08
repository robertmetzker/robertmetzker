
    
    



select count(*) as validation_errors
from (

    select
        NOTE_APP_DTL_LVL_ID

    from STAGING.STG_NOTE_APPLICATION
    where NOTE_APP_DTL_LVL_ID is not null
    group by NOTE_APP_DTL_LVL_ID
    having count(*) > 1

) validation_errors


