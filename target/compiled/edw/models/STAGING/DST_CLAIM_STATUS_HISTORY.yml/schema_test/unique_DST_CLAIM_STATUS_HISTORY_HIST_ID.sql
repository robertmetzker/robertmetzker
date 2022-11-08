
    
    



select count(*) as validation_errors
from (

    select
        HIST_ID

    from STAGING.DST_CLAIM_STATUS_HISTORY
    where HIST_ID is not null
    group by HIST_ID
    having count(*) > 1

) validation_errors


