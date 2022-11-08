
    
    



select count(*) as validation_errors
from (

    select
        HIST_ID

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_HIST
    where HIST_ID is not null
    group by HIST_ID
    having count(*) > 1

) validation_errors


