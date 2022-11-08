
    
    



select count(*) as validation_errors
from STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE
where CDES_ID is null


