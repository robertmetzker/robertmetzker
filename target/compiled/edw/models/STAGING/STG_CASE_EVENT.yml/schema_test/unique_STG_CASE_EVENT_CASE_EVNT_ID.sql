
    
    



select count(*) as validation_errors
from (

    select
        CASE_EVNT_ID

    from STAGING.STG_CASE_EVENT
    where CASE_EVNT_ID is not null
    group by CASE_EVNT_ID
    having count(*) > 1

) validation_errors


