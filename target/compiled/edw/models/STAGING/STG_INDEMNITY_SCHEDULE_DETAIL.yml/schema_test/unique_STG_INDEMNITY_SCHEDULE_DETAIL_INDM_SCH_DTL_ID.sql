
    
    



select count(*) as validation_errors
from (

    select
        INDM_SCH_DTL_ID

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL
    where INDM_SCH_DTL_ID is not null
    group by INDM_SCH_DTL_ID
    having count(*) > 1

) validation_errors


