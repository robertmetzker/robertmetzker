
    
    



select count(*) as validation_errors
from STAGING.DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT
where UNIQUE_ID_KEY is null


