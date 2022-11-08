
    
    



select count(*) as validation_errors
from STAGING.DST_INDEMNITY_PLAN_SCHEDULE_DETAIL
where UNIQUE_ID_KEY is null


