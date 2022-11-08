
    
    



select count(*) as validation_errors
from STAGING.DSV_EXAM_SCHEDULE
where UNIQUE_ID_KEY is null


