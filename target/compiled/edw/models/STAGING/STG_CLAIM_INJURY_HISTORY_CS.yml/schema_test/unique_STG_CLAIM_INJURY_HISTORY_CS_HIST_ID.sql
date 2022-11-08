
    
    



select count(*) as validation_errors
from (

    select
        HIST_ID

    from STAGING.STG_CLAIM_INJURY_HISTORY_CS
    where HIST_ID is not null
    group by HIST_ID
    having count(*) > 1

) validation_errors


