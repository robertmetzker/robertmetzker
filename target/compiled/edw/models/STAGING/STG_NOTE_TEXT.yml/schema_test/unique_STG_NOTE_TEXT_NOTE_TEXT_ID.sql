
    
    



select count(*) as validation_errors
from (

    select
        NOTE_TEXT_ID

    from STAGING.STG_NOTE_TEXT
    where NOTE_TEXT_ID is not null
    group by NOTE_TEXT_ID
    having count(*) > 1

) validation_errors


