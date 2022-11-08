
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE
where EXAM_SCHEDULE_HKEY is null


