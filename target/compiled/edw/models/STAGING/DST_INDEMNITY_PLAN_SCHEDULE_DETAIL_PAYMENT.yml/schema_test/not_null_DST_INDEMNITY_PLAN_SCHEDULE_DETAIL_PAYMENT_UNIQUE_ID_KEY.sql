
    
    



select count(*) as validation_errors
from STAGING.DST_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT
where UNIQUE_ID_KEY is null


