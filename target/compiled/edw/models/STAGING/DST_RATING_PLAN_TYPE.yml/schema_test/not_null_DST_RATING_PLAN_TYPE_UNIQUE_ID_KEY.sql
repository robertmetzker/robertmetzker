
    
    



select count(*) as validation_errors
from STAGING.DST_RATING_PLAN_TYPE
where UNIQUE_ID_KEY is null


