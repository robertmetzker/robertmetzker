
    
    



select count(*) as validation_errors
from (

    select
        CASE_STATUS_HISTORY_ID

    from STAGING.STG_CASE_STATUS_HISTORY
    where CASE_STATUS_HISTORY_ID is not null
    group by CASE_STATUS_HISTORY_ID
    having count(*) > 1

) validation_errors


