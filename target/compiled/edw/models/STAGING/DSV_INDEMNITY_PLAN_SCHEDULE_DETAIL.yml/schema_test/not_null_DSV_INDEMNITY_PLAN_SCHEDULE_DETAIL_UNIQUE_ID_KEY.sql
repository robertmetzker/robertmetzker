
    
    



select count(*) as validation_errors
from STAGING.DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL
where UNIQUE_ID_KEY is null


