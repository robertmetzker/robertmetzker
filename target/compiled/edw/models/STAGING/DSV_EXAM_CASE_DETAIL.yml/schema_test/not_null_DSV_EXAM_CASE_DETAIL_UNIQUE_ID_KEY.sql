
    
    



select count(*) as validation_errors
from STAGING.DSV_EXAM_CASE_DETAIL
where UNIQUE_ID_KEY is null


