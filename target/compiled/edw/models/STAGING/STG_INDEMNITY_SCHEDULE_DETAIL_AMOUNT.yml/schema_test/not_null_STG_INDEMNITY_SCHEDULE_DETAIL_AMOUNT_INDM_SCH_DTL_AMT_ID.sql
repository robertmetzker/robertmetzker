
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL_AMOUNT
where INDM_SCH_DTL_AMT_ID is null


