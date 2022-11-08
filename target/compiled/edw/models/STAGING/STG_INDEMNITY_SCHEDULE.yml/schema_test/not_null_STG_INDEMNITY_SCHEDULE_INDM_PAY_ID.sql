
    
    



select count(*) as validation_errors
from STAGING.STG_INDEMNITY_SCHEDULE
where INDM_PAY_ID is null


