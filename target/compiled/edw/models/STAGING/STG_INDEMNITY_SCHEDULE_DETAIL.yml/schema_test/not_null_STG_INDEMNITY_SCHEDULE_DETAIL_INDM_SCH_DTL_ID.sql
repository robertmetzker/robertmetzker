
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_SCHEDULE_DETAIL
where INDM_SCH_DTL_ID is null

