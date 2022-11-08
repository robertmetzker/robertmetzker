
    
    



select count(*) as validation_errors
from (

    select
        NOTE_ID

    from STAGING.STG_NOTE
    where NOTE_ID is not null
    group by NOTE_ID
    having count(*) > 1

) validation_errors


