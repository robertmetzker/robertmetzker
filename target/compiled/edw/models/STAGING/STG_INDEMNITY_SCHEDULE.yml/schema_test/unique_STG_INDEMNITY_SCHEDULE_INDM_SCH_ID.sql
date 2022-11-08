
    
    



select count(*) as validation_errors
from (

    select
        INDM_SCH_ID

    from STAGING.STG_INDEMNITY_SCHEDULE
    where INDM_SCH_ID is not null
    group by INDM_SCH_ID
    having count(*) > 1

) validation_errors


