
    
    



select count(*) as validation_errors
from (

    select
        INDM_SCH_DTL_AMT_ID

    from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT
    where INDM_SCH_DTL_AMT_ID is not null
    group by INDM_SCH_DTL_AMT_ID
    having count(*) > 1

) validation_errors


